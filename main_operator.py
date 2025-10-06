from qt_pvp.cms_interface import functions as cms_api_funcs
from qt_pvp import functions as main_funcs
from qt_pvp.cms_interface import cms_http
from qt_pvp.cms_interface import cms_api
from qt_pvp import cloud_uploader
from qt_pvp.logger import logger
from qt_pvp import settings
import posixpath
import traceback
import datetime
import asyncio
import shutil
import os


class Main:
    def __init__(self, output_format="mp4"):
        self.jsession = cms_api.login().json()["jsession"]
        #threading.Thread(target=main_funcs.video_remover_cycle).start()
        self.output_format = output_format
        self.devices_in_progress = []
        self.TIME_FMT = "%Y-%m-%d %H:%M:%S"
        self._global_interests_sem = asyncio.Semaphore(settings.config.getint("Process", "MAX_GLOBAL_INTERESTS"))
        self._per_device_sem = {}
        self._devices_sem = asyncio.Semaphore(settings.config.getint("Process", "MAX_DEVICES_CONCURRENT"))

    def _get_device_sem(self, reg_id):
        sem = self._per_device_sem.get(reg_id)
        if sem is None:
            sem = asyncio.Semaphore(settings.config.getint("Process", "MAX_INTERESTS_PER_DEVICE"))
            self._per_device_sem[reg_id] = sem
        return sem

    def video_ready_trigger(self, *args, **kwargs):
        logger.info("Dummy trigger activated")
        pass

    def get_devices_online(self):
        devices_online = cms_api.get_online_devices(self.jsession)
        devices_online = devices_online.json()["onlines"]
        if devices_online:
            logger.info(f"Got devices online: {devices_online}")
        else:
            logger.debug("No devices online (empty 'onlines').")
        return devices_online

    async def operate_device(self, reg_id, plate):
        if reg_id in self.devices_in_progress:
            return
        self.devices_in_progress.append(reg_id)
        try:
            await self.download_reg_videos(reg_id, plate, by_trigger=True)
        except Exception:
            logger.error(traceback.format_exc())
        finally:
            # гарантированно освобождаем
            if reg_id in self.devices_in_progress:
                self.devices_in_progress.remove(reg_id)

    async def get_interests_async(self, reg_id, reg_info, start_time, stop_time):
        """
        Асинхронная версия получения интересов:
        - CMS треки (queryTrackDetail) — в thread-пуле через asyncio.to_thread
        - CMS alarm detail — в thread-пуле через asyncio.to_thread
        - Подготовка алармов/сшивка — синхронно (CPU), можно оставить в основном потоке
        Логика «шага назад по минуте» (max_extra_pulls) сохранена.
        """
        max_extra_pulls = 8  # максимум шагов назад по минуте
        pulls = 0

        while True:
            start_time_dt = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")

            # --- ВАЖНО: обе CMS-функции синхронные -> уводим в thread-пул ---
            tracks = await asyncio.to_thread(
                cms_api.get_device_track_all_pages,
                self.jsession,
                reg_id,
                start_time,
                stop_time
            )

            alarm_reports = await asyncio.to_thread(
                cms_api_funcs.get_device_alarms,
                self.jsession,
                reg_id,  # dev_idno
                None,  # vehi_idno
                start_time,
                stop_time
            )

            prepared = cms_api_funcs.prepare_alarms(
                raw_alarms=alarm_reports.get("alarms", []),
                reg_cfg=reg_info,
                allowed_atp=frozenset({19, 20, 21, 22}),
                min_stop_speed_kmh=settings.config.getint("Interests", "MIN_STOP_SPEED") / 10.0,
                merge_gap_sec=15
            )

            interests = cms_api_funcs.find_interests_by_lifting_switches(
                tracks=tracks,
                start_tracks_search_time=start_time_dt,
                reg_id=reg_id,
                alarms=prepared
            )

            if isinstance(interests, dict) and "interests" in interests:
                return interests["interests"]

            elif isinstance(interests, dict) and "error" in interests:
                pulls += 1
                if pulls > max_extra_pulls:
                    logger.warning(f"[GUARD] Достигнут предел догрузок (pulls={pulls}). Останавливаемся.")
                    return []
                # двигаемся на минуту назад
                start_time = (start_time_dt - datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"Теперь ищем треки с {start_time}")
                continue

            else:
                # На случай иных форматов ответа
                logger.warning(f"[ANALYZE] Неожиданный формат из find_interests_by_lifting_switches: {type(interests)}")
                return []

    async def download_reg_videos(self, reg_id, plate, chanel_id: int = None,
                                  start_time=None, end_time=None,
                                  by_trigger=False, proc=False,
                                  split: int = None):
        logger.debug(f"{reg_id}. Начинаем работу с устройством.")
        begin_time = datetime.datetime.now()

        # Информация о регистраторе
        reg_info = main_funcs.get_reg_info(reg_id) or main_funcs.create_new_reg(reg_id, plate)
        logger.debug(f"{reg_id}. Информация о регистраторе: {reg_info}.")
        chanel_id = reg_info.get("chanel_id", 0)  # Если нет ID канала, ставим 0

        ignore = reg_info.get("ignore", False)
        if ignore:
            logger.debug(f"{reg_id}. Игнорируем регистратор, поскольку в states.json параметр ignore=true.")
            return

        # Временные границы окна
        TIME_FMT = "%Y-%m-%d %H:%M:%S"
        start_time = start_time or main_funcs.get_reg_last_upload_time(reg_id)
        end_time = end_time or begin_time.strftime(TIME_FMT)

        # Разбиваем длинные интервалы на отрезки
        time_difference = (
                datetime.datetime.strptime(end_time, TIME_FMT) -
                datetime.datetime.strptime(start_time, TIME_FMT)
        ).total_seconds()

        max_span = settings.config.getint("Interests", "DOWNLOADING_INTERVAL") * 60
        if time_difference > max_span:
            end_time = (
                    datetime.datetime.strptime(start_time, TIME_FMT) +
                    datetime.timedelta(seconds=max_span)
            ).strftime(TIME_FMT)
        else:
            logger.debug(f"f{reg_id}. Time difference is too short ({time_difference} сек.)")
            return

        #max_span = settings.config.getint("Interests", "DOWNLOADING_INTERVAL") * 60
        #if time_difference <= 0:
        #    logger.debug(f"{reg_id}. Пустое окно ({time_difference} сек.).")
        #    return
        #if time_difference > max_span:
        #    end_time = (datetime.datetime.strptime(start_time, TIME_FMT) +
        #                datetime.timedelta(seconds=max_span)).strftime(TIME_FMT)
        # иначе оставляем end_time как есть и работаем с «коротким» окном


        logger.info(f"{reg_id} Начало: {start_time}, Конец: {end_time}")

        # Определяем интересные интервалы
        interests = await self.get_interests_async(reg_id, reg_info, start_time, end_time)
        if not interests:
            logger.info(f"{reg_id}: Интересы не найдены в интервале {start_time} - {end_time}")
            main_funcs.save_new_reg_last_upload_time(reg_id, end_time)
            return True

        logger.info(f"{reg_id}: Найдено {len(interests)} интересов")
        interests = main_funcs.merge_overlapping_interests(interests)
        interests = main_funcs.filter_already_processed(reg_id, interests)
        logger.info(f"{reg_id}: К запуску {len(interests)} интересов (после фильтра processed).")

        async def _process_one_interest(interest: dict) -> str | None:
            # ограничители: глобально и на устройство
            async with self._global_interests_sem, self._get_device_sem(reg_id):
                created_start_time = datetime.datetime.now()
                interest_name = interest["name"]

                # Создаём пути в облаке под интерес
                cloud_paths = await cloud_uploader.create_interest_folder_path_async(
                    name=interest_name,
                    dest=settings.CLOUD_PATH
                )
                if not cloud_paths:
                    logger.error(f"{reg_id}: Не удалось создать папки для {interest_name}. Пропускаем интерес.")
                    return interest["end_time"]

                interest_cloud_folder = cloud_paths["interest_folder_path"]
                interest["cloud_folder"] = interest_cloud_folder

                # 1) проверяем наличие полного видео интереса в облаке
                interest_video_exists = await asyncio.to_thread(
                    cloud_uploader.check_if_interest_video_exists,
                    interest_name
                )

                # 2) какие каналы нужны для кадров
                before_channels_to_download, after_channels_to_download = await self.get_channels_to_download_pics(
                    interest_cloud_folder
                )

                # 3) если видео по интересу в облаке НЕТ — добавляем канал полного ролика
                to_download_for_full_clip = [chanel_id] if not interest_video_exists else []

                # детерминированное объединение без дублей
                final_channels_to_download = sorted({
                    *before_channels_to_download,
                    *after_channels_to_download,
                    *to_download_for_full_clip
                })

                logger.debug(
                    f"{reg_id}. Нужно скачать видео интереса: {not interest_video_exists}. "
                    f"Кадры ДО: {before_channels_to_download}. "
                    f"Кадры ПОСЛЕ: {after_channels_to_download}. "
                    f"Итого каналы: {final_channels_to_download}"
                )

                # 4) скачиваем по одному клипу на канал
                channels_files_dict = await cms_api.download_single_clip_per_channel(
                    jsession=self.jsession,
                    reg_id=reg_id,
                    interest=interest,
                    channels=final_channels_to_download
                )

                # 5) если надо — выгружаем «полный» клип в облако (только для chanel_id)
                if not interest_video_exists:
                    full_clip_path = channels_files_dict.get(chanel_id)
                    if full_clip_path:
                        upload_status = await self.upload_interest_video_cloud(
                            reg_id=reg_id,
                            interest_name=interest_name,
                            video_path=full_clip_path,
                            cloud_folder=cloud_paths["interest_folder_path"]
                        )
                    else:
                        logger.warning(
                            f"{reg_id}: Полный клип по каналу {chanel_id} не получен — пропускаем загрузку видео.")

                # 6) извлекаем кадры из КАЖДОГО скачанного клипа и выгружаем их
                if settings.config.getboolean("General", "pics_before_after"):
                    upload_status = await self.process_frames_before_after_v2(
                        reg_id, interest, channels_files_dict  # ← передаём словарь!!!
                    )
                    if upload_status["upload_status"]:
                        for frame in upload_status["frames_before"] + upload_status["frames_after"]:
                            logger.info(f"{reg_id}: Загрузка фото ок. Удаляем локальный файл {frame}.")

                    # 7) чистим локальные клипы (кроме «полного» по нужному каналу)
                    removed = cms_api.delete_videos_except(
                        videos_by_channel=channels_files_dict,
                        keep_channel_id=chanel_id if not interest_video_exists else None
                    )

                    # И только теперь можем удалить основное видео интереса
                    if upload_status:
                        if upload_status:
                            logger.info(f"{reg_id}: Загрузка видео интереса {interest_name} прошла успешно.")
                            if settings.config.getboolean("General", "del_source_video_after_upload"):
                                if os.path.exists(full_clip_path):
                                    logger.info(
                                        f"{reg_id}: Удаляем локальное видео интереса {interest_name}. ({video_path}).")
                                    os.remove(full_clip_path)
                                interest_temp_folder = os.path.join(settings.TEMP_FOLDER,
                                                                    interest_name)
                                if os.path.exists(interest_temp_folder):
                                    logger.info(
                                        f"{reg_id}: Удаляем временную директорию интереса {interest_name}. ({interest_temp_folder}).")
                                    shutil.rmtree(interest_temp_folder)
                        else:
                            logger.error(f"{reg_id}: Ошибка загрузки {interest_name}.")
                    logger.info(f"{reg_id}: V2 завершено. Upload={upload_status}. Удалено видеофайлов: {removed}.")


                await cloud_uploader.append_report_line_to_cloud_async(
                    remote_folder_path=cloud_paths["date_folder_path"],
                    created_start_time=created_start_time.strftime(self.TIME_FMT),
                    created_end_time=datetime.datetime.now().strftime(self.TIME_FMT),
                    file_name=interest_name
                )

                # Маркируем интерес как обработанный (локально)
                #main_funcs._save_processed(reg_id, interest_name)

                return interest["end_time"]

        # Стартуем задачи (сами ограничители внутри)
        tasks = [asyncio.create_task(_process_one_interest(it)) for it in interests]

        # Собираем результаты по мере готовности
        end_times: list[str] = []
        try:
            for coro in asyncio.as_completed(tasks):
                try:
                    et = await coro
                    if et:
                        end_times.append(et)
                except cms_api.DeviceOfflineError as err:
                    logger.debug(f"{reg_id}. Устройство оффлайн, прерываем обработку интересов.")

                    # отменяем все остальные задачи
                    for t in tasks:
                        t.cancel()

                    # ждём, пока они корректно завершатся (с подавлением CancelledError)
                    await asyncio.gather(*tasks, return_exceptions=True)
                    logger.error(f"{reg_id}. Обработка регистратора завершена.")
                    return {"error": "Device offline error"}
                except Exception:
                    logger.error(f"{reg_id}: Ошибка в задаче интереса:\n{traceback.format_exc()}")
        finally:
            # на всякий случай — чтобы не остались висячие задачи
            for t in tasks:
                if not t.done():
                    t.cancel()

        # Обновляем last_upload_time ОДИН раз — максимумом из завершённых интересов,
        # либо (если все упали/ничего не пришло) — концом окна end_time
        if end_times:
            new_last = max(end_times)
        else:
            new_last = start_time  # конец окна?, чтобы не зациклиться на том же диапазоне
        #main_funcs.save_new_reg_last_upload_time(reg_id, new_last)
        main_funcs.save_new_reg_last_upload_time(reg_id, new_last)
        logger.info(
            f"{reg_id}. Пакет интересов завершён: {len(end_times)}/{len(interests)}; last_upload_time -> {new_last}")

    async def get_channels_to_download_pics(self, interest_cloud_path):
        pics_after_folder = posixpath.join(interest_cloud_path, "after_pics")
        pics_before_folder = posixpath.join(interest_cloud_path, "before_pics")

        channels = [0, 1, 2, 3]

        # Параллельные проверки наличия на облаке
        before_checks = [asyncio.create_task(cloud_uploader._frame_exists_cloud_async(pics_before_folder, ch)) for ch in channels]
        after_checks = [asyncio.create_task(cloud_uploader._frame_exists_cloud_async(pics_after_folder, ch)) for ch in channels]

        before_exists = await asyncio.gather(*before_checks)
        after_exists = await asyncio.gather(*after_checks)

        before_channels_to_download = [ch for ch, exists in zip(channels, before_exists) if not exists]
        after_channels_to_download = [ch for ch, exists in zip(channels, after_exists) if not exists]
        return before_channels_to_download, after_channels_to_download

    # --- ДОБАВИТЬ В КЛАСС Main (main_operator.py) ---
    async def process_frames_before_after_v2(self, reg_id: str, enriched: dict, videos_by_channel):
        """
        ВЕРСИЯ 2:
        1) Скачиваем по ОДНОМУ клипу на канал, покрывающему [start_time; end_time]
        2) Из каждого клипа берём первый и последний кадр
        3) Заливаем в облако в before_pics / after_pics
        4) Удаляем все локальные клипы, кроме выбранного канала (опционально)
        Возвращает: {"upload_status": bool, "frames_before": [...], "frames_after": [...]}
        """
        interest_folder_path = enriched["cloud_folder"]
        pics_after_folder = posixpath.join(interest_folder_path, "after_pics")
        pics_before_folder = posixpath.join(interest_folder_path, "before_pics")
        channels = [0, 1, 2, 3]

        # 2) Достаём из каждого клипа первый/последний кадр
        frames_before: list[str] = []
        frames_after: list[str] = []

        async def _extract_for_channel(ch: int, path: str | None):
            if not path:
                return None, None
            return await cms_api.extract_edge_frames_from_video(
                video_path=path,
                channel_id=ch,
                reg_id=reg_id,
            )

        extract_tasks = [asyncio.create_task(_extract_for_channel(ch, videos_by_channel.get(ch))) for ch in channels]
        for ch, t in zip(channels, asyncio.as_completed(extract_tasks)):
            first_path, last_path = await t
            if first_path:
                frames_before.append(first_path)
            if last_path:
                frames_after.append(last_path)

        # 3) Заливка кадров в облако — та же функция, что и раньше  :contentReference[oaicite:7]{index=7}
        upload_status = await asyncio.to_thread(
            cloud_uploader.create_pics,
            frames_before, frames_after, pics_before_folder, pics_after_folder
        )
        return {"upload_status": upload_status, "frames_before": frames_before, "frames_after": frames_after}


    async def process_frames_before_after(self, reg_id, enriched):
        interest_folder_path = enriched["cloud_folder"]
        before_channels_to_download, after_channels_to_download = await self.get_channels_to_download_pics(interest_folder_path)
        logger.debug(f"{reg_id}: интерес {enriched['name']}. Для скачивания определены кадры ДО - {before_channels_to_download}. "
                     f"После - {after_channels_to_download}")

        frames_before: list[str] = []
        frames_after: list[str] = []

        logger.debug(f"{reg_id}: интерес {enriched['name']}. Получаем кадры ДО и ПОСЛЕ загрузки")

        if before_channels_to_download:
            frames_before = await cms_api.get_frames(
                jsession=self.jsession, reg_id=reg_id,
                year=enriched["year"], month=enriched["month"], day=enriched["day"],
                start_sec=enriched["photo_before_sec"],
                end_sec=enriched["photo_before_sec"] + 10,
                channels=before_channels_to_download,
            )
            logger.debug(f"{reg_id}: интерес {enriched['name']}. Кадры ДО: {frames_before}")
        else:
            logger.info(f"{reg_id}: интерес {enriched['name']}. Все фото ДО по интересу уже загружены на облако")

        if after_channels_to_download:
            frames_after = await cms_api.get_frames(
                jsession=self.jsession, reg_id=reg_id,
                year=enriched["year"], month=enriched["month"], day=enriched["day"],
                start_sec=enriched["photo_after_sec"],
                end_sec=enriched["photo_after_sec"] + 10,
                channels=after_channels_to_download,
            )
            logger.debug(f"{reg_id}: интерес {enriched['name']}. Фото ПОСЛЕ: {frames_after}")
        else:
            logger.info(f"{reg_id}: интерес {enriched['name']}. Все фото ПОСЛЕ по интересу уже загружены на облако")

        upload_status = await asyncio.to_thread(
            cloud_uploader.create_pics,
            frames_before, frames_after
        )
        return {"upload_status": upload_status, "frames_before": frames_before, "frames_after": frames_after}

    def analyze_frames_quality(self, frames: list):
        """
        Проверяет список кадров: сколько настоящих фото и сколько заглушек.
        """
        real_photos = 0
        placeholders = 0

        for frame_path in frames:
            if "placeholder" in os.path.basename(frame_path).lower():
                placeholders += 1
            else:
                real_photos += 1

        logger.info(
            f"Качество кадров: Реальные фото: {real_photos}, Заглушки: {placeholders}")
        return {
            "real_photos": real_photos,
            "placeholders": placeholders,
            "total": len(frames)
        }

    async def process_and_upload_videos_async(self, reg_id, interest):
        interest_name = interest["name"]
        file_paths = interest.get("file_paths", [])
        if not file_paths:
            logger.warning(
                f"{reg_id}: Нет видео для {interest_name}. Пропускаем.")
            return

        video_task = asyncio.create_task(
            self.process_video_and_return_path(reg_id, interest,
                                               file_paths)
        )

        # Дожидаемся завершения обеих задач
        result = await video_task

        if "error" in result:
            logger.error(f"{reg_id}. Ошибка при обработке видео интереса {interest_name}:  {result["error"]}")
            return
        if not result["output_video_path"]:
            logger.warning(
                f"{reg_id}: Нечего выгружать на облако.")
            return
        await self.upload_interest_video_cloud(reg_id=reg_id, interest_name=interest_name,
                                         video_path=result["output_video_path"], cloud_folder=interest["cloud_folder"])

    async def upload_interest_video_cloud(self, reg_id, interest_name, video_path, cloud_folder):
        # Загружаем видео
        logger.info(
            f"{reg_id}: Загружаем видео интереса {interest_name} в облако.")
        upload_status = await asyncio.to_thread(
            cloud_uploader.upload_file, video_path, cloud_folder)
        return upload_status

    async def process_video_and_return_path(self, reg_id, interest,
                                            file_paths):
        """Обрабатывает видео и возвращает путь к финальному файлу."""
        logger.info(
            f"{reg_id}: Начинаем обработку видео {file_paths} для {interest['name']}.")
        interest_name = interest["name"]

        final_interest_video_name = os.path.join(
            settings.INTERESTING_VIDEOS_FOLDER,
            f"{interest_name}.{self.output_format}")
        result = {"output_video_path": final_interest_video_name,
                  "files_to_delete": []}
        converted_videos = []
        for video_path in file_paths:
            logger.debug(f"Работаем с {video_path}")
            if not os.path.exists(video_path):
                logger.error(
                    f"{reg_id}: Файл {video_path} не найден. Пропускаем.")
                continue
            #logger.info(
            #    f"{reg_id}: Конвертация {video_path} в {self.output_format}.")
            #output_filename = os.path.join(
            #    settings.INTERESTING_VIDEOS_FOLDER,
            ##    f"{interest_name}_{file_paths.index(video_path)}.{self.output_format}")
            #converted_video = main_funcs.process_video_file(
            #    video_path, output_filename)
            #if converted_video:
            #    converted_videos.append(converted_video)

        final_videos_paths_list = (converted_videos if converted_videos else file_paths)
        if len(final_videos_paths_list) > 1:
            try:
                await asyncio.to_thread(main_funcs.concatenate_videos,
                                    final_videos_paths_list,
                                    final_interest_video_name)
            except Exception as e:
                return {"error": str(e)}
            if converted_videos and settings.config.getboolean("General", "del_source_video_after_upload"):
                logger.debug("Конвертированные файлы исходники перед конкатенацией добавляем в список удаления")
                for file in final_videos_paths_list:
                    if os.path.exists(file):
                        result["files_to_delete"].append(file)
        elif len(final_videos_paths_list) == 1:
            output_video_path = final_videos_paths_list[
                0]  # Если одно видео, просто используем его
            if os.path.exists(output_video_path):
                os.rename(output_video_path, final_interest_video_name)
            else:
                logger.error(f"Ошибка при попытке использовать видео {output_video_path}. Файл не найден.")
        else:
            logger.warning(f"{reg_id}: После обработки не осталось видео.")
            result["output_video_path"] = None
            return result

        if converted_videos and settings.config.getboolean("General", "del_source_video_after_upload"):
            logger.debug("Исходные файлы до конвертации добавляем в список удаления")
            for file in file_paths:
                if os.path.exists(file):
                    result["files_to_delete"].append(file)
        return result

    def get_last_interest_datetime(self, interests):
        last_interest = interests[-1]
        return last_interest["end_time"]

    async def mainloop(self):
        logger.info("Mainloop has been launched with success.")
        self._running: set[asyncio.Task] = set()

        while True:
            # важно: get_devices_online в thread, чтобы не блокировать loop
            devices_online = await asyncio.to_thread(self.get_devices_online)

            for device_dict in devices_online:
                reg_id = device_dict["did"]
                plate = device_dict["vid"]

                # если девайс уже в работе — пропускаем
                if reg_id in self.devices_in_progress:
                    continue

                async def _run_with_limit(rid, pl):
                    async with self._devices_sem:
                        await self.operate_device(rid, pl)

                # Стартуем корутину и НЕ ждём всю пачку
                t = asyncio.create_task(_run_with_limit(reg_id, plate))
                self._running.add(t)
                t.add_done_callback(self._running.discard)

            await asyncio.sleep(3)

        cms_http.close_cms_async_client()


if __name__ == "__main__":
    d = Main()
    asyncio.run(d.mainloop())
