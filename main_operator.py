from qt_pvp.cms_interface import functions as cms_api_funcs
from qt_pvp import functions as main_funcs
from qt_pvp.cms_interface import cms_api
from qt_pvp import cloud_uploader
from qt_pvp.logger import logger
from qt_pvp import settings
import asyncio
import traceback
import datetime
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
            logger.debug(f"Got devices online: {devices_online}")
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

    def get_interests(self, reg_id, reg_info, start_time, stop_time):
        max_extra_pulls = 8  # максимум шагов назад по минуте
        pulls = 0

        while True:
            start_time_dt = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")

            tracks = cms_api.get_device_track_all_pages(
                jsession=self.jsession,
                device_id=reg_id,
                start_time=start_time,
                stop_time=stop_time,
            )
            alarm_reports = cms_api_funcs.get_device_alarms(
                jsession=self.jsession,
                dev_idno=reg_id,
                begintime=start_time,
                endtime=stop_time)
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

            if "interests" in interests:
                return interests["interests"]

            elif "error" in interests:
                pulls += 1
                if pulls > max_extra_pulls:
                    logger.warning(f"[GUARD] Достигнут предел догрузок (pulls={pulls}). Останавливаемся.")
                    return []
                # двигаемся на минуту назад
                start_time = (start_time_dt - datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"Теперь ищем треки с {start_time}")

            else:
                # На случай иных форматов ответа
                logger.warning(f"[ANALYZE] Неожиданный формат: {type(interests)}")
                return []

    async def download_reg_videos(self, reg_id, plate, chanel_id: int = None,
                                  start_time=None, end_time=None,
                                  by_trigger=False, proc=False,
                                  split: int = None):
        logger.debug(f"Начинаем работу с устройством {reg_id}")
        begin_time = datetime.datetime.now()

        # Проверка доступности регистратора
        #if not self.check_if_reg_online(reg_id):
        #    logger.info(f"{reg_id} недоступен.")
        #    return

        # Информация о регистраторе
        reg_info = main_funcs.get_reg_info(reg_id) or main_funcs.create_new_reg(reg_id, plate)
        logger.debug(f"Информация о регистраторе {reg_id} - {reg_info}")
        #if not reg_info:
        #    main_funcs.create_new_reg(reg_id)
        chanel_id = reg_info.get("chanel_id", 0)  # Если нет ID канала, ставим 0

        ignore = reg_info.get("ignore", False)
        if ignore:
            logger.debug(f"Игнорируем регистратор {reg_id}")
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

        logger.info(f"{reg_id} Начало: {start_time}, Конец: {end_time}")

        # Определяем интересные интервалы
        interests = self.get_interests(reg_id, reg_info, start_time, end_time)
        if not interests:
            logger.info(f"{reg_id}: Интересы не найдены в интервале {start_time} - {end_time}")
            main_funcs.save_new_reg_last_upload_time(reg_id, end_time)
            return True

        logger.info(f"{reg_id}: Найдено {len(interests)} интересов")
        interests = main_funcs.merge_overlapping_interests(interests)
        interests = main_funcs.filter_already_processed(reg_id, interests)
        logger.info(f"{reg_id}: К запуску {len(interests)} интересов (после фильтра processed).")

        # Единая стратегия расширения окна — теперь задаём здесь,
        # а применяет её download_video (через download_interest_videos).
        adjustment_sequence = (0, 15, 30, 45)

        async def _process_one_interest(interest: dict) -> str | None:
            # ограничители: глобально и на устройство
            async with self._global_interests_sem, self._get_device_sem(reg_id):
                created_start_time = datetime.datetime.now()
                interest_name = interest["name"]

                # Дедуп по облаку (быстрый выход)
                if cloud_uploader.interest_folder_exists(interest_name, settings.CLOUD_PATH):
                    logger.info(f"[DEDUP] В облаке уже есть папка интереса {interest_name} — пропускаем.")
                    main_funcs._save_processed(reg_id, interest_name)
                    return interest["end_time"]  # вернём конец интереса для будущего max()

                # Создаём пути в облаке под интерес
                cloud_paths = cloud_uploader.create_interest_folder_path(
                    interest_name=interest_name,
                    dest_directory=settings.CLOUD_PATH
                )
                if not cloud_paths:
                    logger.error(f"{reg_id}: Не удалось создать папки для {interest_name}. Пропускаем интерес.")
                    return interest["end_time"]

                interest_cloud_folder = cloud_paths["interest_folder_path"]

                logger.debug(f"{reg_id}: Начинаем скачивание видео для {interest_name}")
                enriched = await cms_api.download_interest_videos(
                    self.jsession, interest, chanel_id, reg_id=reg_id,
                    adjustment_sequence=adjustment_sequence,
                )  # вернёт interest с file_paths, либо None :contentReference[oaicite:3]{index=3}

                if not enriched:
                    logger.warning(f"{reg_id}: Не удалось получить видеофайлы для {interest_name}")
                    # Важно: всё равно вернём end_time, чтобы батч мог продвинуть last_upload_time вперёд
                    return interest["end_time"]

                enriched["cloud_folder"] = interest_cloud_folder

                # Обработка и загрузка видео (как у тебя сейчас)
                await self.process_and_upload_videos_async(reg_id, enriched)

                # Кадры до/после — оставляем как есть (пока без параллели каналов)
                if settings.config.getboolean("General", "pics_before_after"):
                    upload_status = await self.upload_frames_before_after(reg_id, enriched)
                    if upload_status["upload_status"]:
                        for frame in upload_status["frames_before"] + upload_status["frames_after"]:
                            logger.info(f"{reg_id}: Загрузка фото ок. Удаляем локальный файл {frame}.")

                # Отчёты
                cloud_uploader.upload_dict_as_json_to_cloud(
                    data=enriched["report"], remote_folder_path=enriched["cloud_folder"]
                )
                cloud_uploader.append_report_line_to_cloud(
                    remote_folder_path=cloud_paths["date_forder_path"],
                    created_start_time=created_start_time.strftime(self.TIME_FMT),
                    created_end_time=datetime.datetime.now().strftime(self.TIME_FMT),
                    file_name=interest_name
                )

                # Маркируем интерес как обработанный (локально)
                main_funcs._save_processed(reg_id, interest_name)

                return interest["end_time"]

        # Стартуем задачи (сами ограничители внутри)
        tasks = [asyncio.create_task(_process_one_interest(it)) for it in interests]

        # Собираем результаты по мере готовности
        end_times: list[str] = []
        for coro in asyncio.as_completed(tasks):
            try:
                et = await coro
                if et:
                    end_times.append(et)
            except Exception:
                logger.error(f"{reg_id}: Ошибка в задаче интереса:\n{traceback.format_exc()}")

        # Обновляем last_upload_time ОДИН раз — максимумом из завершённых интересов,
        # либо (если все упали/ничего не пришло) — концом окна end_time
        if end_times:
            new_last = max(end_times)
        else:
            new_last = end_time  # конец окна, чтобы не зациклиться на том же диапазоне
        main_funcs.save_new_reg_last_upload_time(reg_id, new_last)

        logger.info(
            f"{reg_id}. Пакет интересов завершён: {len(end_times)}/{len(interests)}; last_upload_time -> {new_last}")



    async def upload_frames_before_after(self, reg_id, enriched):
        logger.debug("Получаем кадры ДО и ПОСЛЕ загрузки")
        frames_before = await cms_api.get_frames(
            jsession=self.jsession, reg_id=reg_id,
            year=enriched["year"], month=enriched["month"],
            day=enriched["day"],
            start_sec=enriched["photo_before_sec"],
            end_sec=enriched["photo_before_sec"] + 10
        )
        logger.debug(f"Кадры до: {frames_before}")
        frames_after = await cms_api.get_frames(
            jsession=self.jsession, reg_id=reg_id,
            year=enriched["year"], month=enriched["month"],
            day=enriched["day"],
            start_sec=enriched["photo_after_sec"],
            end_sec=enriched["photo_after_sec"] + 10
        )
        logger.debug(f"Фото до - {frames_before}. Фото после - {frames_after}")

        quality_report = self.analyze_frames_quality(frames_before + frames_after)
        logger.info(f"Анализ качества фото: {quality_report}")

        upload_status = await asyncio.to_thread(
            cloud_uploader.create_pics, enriched["cloud_folder"],
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
                f"{reg_id}: Нет видеофайлов для {interest_name}. Пропускаем.")
            return

        video_task = asyncio.create_task(
            self.process_video_and_return_path(reg_id, interest,
                                               file_paths)
        )

        # Дожидаемся завершения обеих задач
        result = await video_task

        if "error" in result:
            logger.error(result["error"])
            return
        if not result["output_video_path"]:
            logger.warning(
                f"{reg_id}: Нечего выгружать на облако.")
            return

        # Загружаем видео
        logger.info(
            f"{reg_id}: Загружаем видео {interest_name} в облако.")
        upload_status = await asyncio.to_thread(
            cloud_uploader.upload_file, result["output_video_path"],
            interest["cloud_folder"]
        )

        if upload_status:
            logger.info(f"{reg_id}: Загрузка прошла успешно.")
            if settings.config.getboolean("General", "del_source_video_after_upload"):
                if os.path.exists(result["output_video_path"]):
                    logger.info(f"{reg_id}: Удаляем локальный файл ({result['output_video_path']}).")
                    os.remove(result["output_video_path"])
                    for file_path in result["files_to_delete"]:
                        if os.path.exists(file_path):
                            logger.debug(f"Удаляем {file_path}")
                            os.remove(file_path)
                interest_temp_folder = os.path.join(settings.TEMP_FOLDER,
                                           interest_name)
                if os.path.exists(interest_temp_folder):
                    logger.info(f"{reg_id}: Удаляем временную директорию интереса.")
                    shutil.rmtree(interest_temp_folder)
        else:
            logger.error(f"{reg_id}: Ошибка загрузки {interest_name}.")

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
            logger.info(
                f"{reg_id}: Конвертация {video_path} в {self.output_format}.")
            output_filename = os.path.join(
                settings.INTERESTING_VIDEOS_FOLDER,
                f"{interest_name}_{file_paths.index(video_path)}.{self.output_format}")
            converted_video = main_funcs.process_video_file(
                video_path, output_filename)
            if converted_video:
                converted_videos.append(converted_video)

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
        while True:
            devices_online = self.get_devices_online()
            for device_dict in devices_online:
                reg_id = device_dict["did"]
                plate = device_dict["vid"]
                await self.operate_device(reg_id, plate)
            await asyncio.sleep(3)

    def check_if_reg_online(self, reg_id):
        devices_online = self.get_devices_online()
        for device_dict in devices_online:
            if reg_id == device_dict["did"]:
                return True


if __name__ == "__main__":
    d = Main()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(d.mainloop())
    # b = d.trace_reg_state("104039")
    # 118270348452
    # 2024050601
