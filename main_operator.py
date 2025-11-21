from qt_pvp.interest_merge_funcs import merge_overlapping_interests
from qt_pvp.cms_interface import functions as cms_api_funcs
from qt_pvp.qt_rm_client import QTRMAsyncClient
from qt_pvp import functions as main_funcs
from qt_pvp.cms_interface import cms_http
from qt_pvp.cms_interface import cms_api
from qt_pvp import cloud_uploader
from qt_pvp.logger import logger
from qt_pvp.data import settings
from qt_pvp import geo_funcs
import posixpath
import traceback
import datetime
import asyncio
import shutil
import os


class Main:
    def __init__(self, output_format="mp4"):
        #threading.Thread(target=main_funcs.video_remover_cycle).start()
        self.output_format = output_format
        self.devices_in_progress = []
        self.TIME_FMT = "%Y-%m-%d %H:%M:%S"
        self._global_interests_sem = asyncio.Semaphore(settings.config.getint("Process", "MAX_GLOBAL_INTERESTS"))
        self._per_device_sem = {}
        self._devices_sem = asyncio.Semaphore(settings.config.getint("Process", "MAX_DEVICES_CONCURRENT"))
        self.ignore_points = geo_funcs.get_ignore_points()
        self._interest_refill_in_progress = set()
        self.qt_rm_client = QTRMAsyncClient(
            base_url=settings.qt_rm_url,
            username=settings.qt_rm_login,
            password=settings.qt_rm_password,
            concurrent_requests=settings.config.getint("QT_RM", "CONCURRENT_REQUESTS", fallback=16),)

    def _get_device_sem(self, reg_id):
        sem = self._per_device_sem.get(reg_id)
        if sem is None:
            sem = asyncio.Semaphore(settings.config.getint("Process", "MAX_INTERESTS_PER_DEVICE"))
            self._per_device_sem[reg_id] = sem
        return sem

    async def get_devices_online(self):
        devices_online = await cms_api.get_online_devices(self.jsession)
        devices_online = devices_online.json()["onlines"]
        if devices_online:
            logger.debug(f"Got devices online: {devices_online}")
        else:
            logger.debug("No devices online (empty 'onlines').")
        return devices_online

    async def operate_device(self, reg_id, plate):
        if reg_id in self.devices_in_progress:
            return
        self.devices_in_progress.append(reg_id)
        try:
            await self.download_reg_videos(reg_id, plate)
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

            tracks_task = asyncio.create_task(cms_api.get_device_track_all_pages_async(
                self.jsession, reg_id, start_time, stop_time))
            alarms_task = asyncio.create_task(cms_api.get_device_alarm_all_pages_async(self.jsession, reg_id, start_time, stop_time))
            tracks, alarm_reports = await asyncio.gather(tracks_task, alarms_task)
            tracks = [t for page in tracks for t in (page.get("tracks") or [])]
            all_alarms = []
            for page in alarm_reports:
                all_alarms.extend(page.get("alarms") or [])
            #for alarm in all_alarms:
            #    print(alarm)

            prepared = cms_api_funcs.prepare_alarms(
                raw_alarms=all_alarms,
                reg_cfg=reg_info,
                allowed_atp=frozenset({19, 20, 21, 22}),
                min_stop_speed_kmh=settings.config.getint("Interests", "MIN_STOP_SPEED") / 10.0,
                merge_gap_sec=15,
                reg_id=reg_id
            )

            try:
                interests = cms_api_funcs.find_interests_by_lifting_switches(
                    tracks=tracks,
                    start_tracks_search_time=start_time_dt,
                    reg_id=reg_id,
                    alarms=prepared,
                )
            except cms_api_funcs.LoadingInProgress:
                logger.info("Прерываем обработку интересов потому что машина грузится в это время ")
                return {"error": "Loading in progress"}

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

    def _parse_start_ts(self, it: dict):
        try:
            return datetime.datetime.strptime(it.get("start_time", ""), settings.TIME_FMT)
        except Exception:
            return datetime.datetime.max  # если испорченный интерес — обрабатываем в самом конце

    async def download_reg_videos(self, reg_id, plate):
        logger.debug(f"{reg_id}. Начинаем работу с устройством.")

        # Информация о регистраторе
        reg_info = main_funcs.get_reg_info(reg_id) or main_funcs.create_new_reg(reg_id, plate)
        logger.debug(f"{reg_id}. Информация о регистраторе: {reg_info}.")

        ignore = reg_info.get("ignore", False)
        if ignore:
            logger.debug(f"{reg_id}. Игнорируем регистратор, поскольку в states.json параметр ignore=true.")
            return

        # Pending - уже извлеченные из CMS и сохраненные в states.json интересы
        pending = main_funcs.get_pending_interests(reg_id)
        if not pending:
            await self._refill_pending_interests_if_due(reg_id)     # Извлечь новые интересы из CMS
            pending = main_funcs.get_pending_interests(reg_id)

        if pending:
            interests = pending
            logger.info(f"{reg_id}: Берём {len(interests)} интерес(а/ов) из очереди pending_interests.")
            logger.debug(f"({interests})")
        else:
            # Если очередь пуста и проверка давности не прошла — делать лишних запросов не будем
            logger.info(f"{reg_id}: Очередь pending_interests пуста, и наполнять сейчас рано — завершаем.")
            return True

        logger.info(f"{reg_id}: Найдено {len(interests)} интересов")
        interests = merge_overlapping_interests(interests)
        logger.info(f"{reg_id}: К запуску {len(interests)} интересов (после фильтра processed).")

        # сортируем интересы по времени начала, старые сначала
        interests.sort(key=self._parse_start_ts)

        total_found = len(interests)
        max_per_batch = settings.config.getint("Interests", "MAX_INTERESTS_PER_BATCH", fallback=8)
        if total_found > max_per_batch:
            logger.info(
                f"{reg_id}: Берём в работу только {max_per_batch} из {total_found} интересов (батч). "
                f"Остальные — в следующий цикл."
            )
            interests = interests[:max_per_batch]
        else:
            logger.info(f"{reg_id}: Влезают все интересы ({total_found}) в одну пачку.")

        # Стартуем задачи (сами ограничители внутри)
        channel_id = reg_info.get("chanel_id")
        tasks = [asyncio.create_task(self._process_one_interest(it, channel_id)) for it in interests]

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
        logger.info(f"{reg_id}: Пакет интересов завершён: {len(end_times)}/{len(interests)}")


    async def _process_one_interest(self, interest: dict, channel_id) -> str | None:
        reg_id = interest.get("reg_id")
        async with self._global_interests_sem, self._get_device_sem(reg_id): # Ограничители глобально и по устройство
            created_start_time = datetime.datetime.now()
            interest_name = interest["name"]
            nearby_point =  geo_funcs.find_nearby_name(
                interest["report"]["geo"], self.ignore_points,
                settings.config.getint("Interests", "IGNORE_POINTS_TOLERANCE"))
            if nearby_point:
                logger.info(f"{reg_id}: Пропускаем интерес {interest_name}, "
                            f"интерес зафиксирован рядом {interest['report']['geo']} с точкой игнора - {nearby_point}")
                self.del_pending_interest(reg_id, interest_name)
                return None

            logger.info(f"{reg_id}: Начинаем работу с интересом {interest_name}")
            logger.debug(f"{interest}")

            # Создаём пути в облаке под интерес
            cloud_paths = await cloud_uploader.create_interest_folder_path_async(
                name=interest_name,
                dest=settings.CLOUD_PATH
            )

            if not cloud_paths:
                logger.error(f"{reg_id}: Не удалось создать папки для {interest_name}. Пропускаем интерес.")
                self.del_pending_interest(reg_id, interest_name)
                return interest["end_time"]

            interest_cloud_folder = cloud_paths["interest_folder_path"]
            interest["cloud_folder"] = interest_cloud_folder
            pics_after_folder = posixpath.join(interest_cloud_folder, "after_pics")
            pics_before_folder = posixpath.join(interest_cloud_folder, "before_pics")
            interest["pics_before_folder"] = pics_before_folder
            interest["pics_after_folder"] = pics_after_folder
            await cloud_uploader.acreate_folder_if_not_exists(cloud_uploader.client, pics_before_folder)
            await cloud_uploader.acreate_folder_if_not_exists(cloud_uploader.client, pics_after_folder)

            # 1) проверяем наличие полного видео интереса в облаке
            interest_video_exists = await cloud_uploader.check_if_interest_video_exists(interest_name)

            # 2) какие каналы нужны для кадров
            before_channels_to_download, after_channels_to_download = await self.get_channels_to_download_pics(
                interest_cloud_folder
            )

            # 3) если видео по интересу в облаке НЕТ — добавляем канал полного ролика
            to_download_for_full_clip = [channel_id] if not interest_video_exists else []
            logger.debug(f"BEFORE,AFTER,FULL: {before_channels_to_download}, {after_channels_to_download}, {to_download_for_full_clip}")
            # детерминированное объединение без дублей
            final_channels_to_download = sorted({
                *before_channels_to_download,
                *after_channels_to_download,
                *to_download_for_full_clip
            })

            logger.debug(
                f"{reg_id}. {interest_name} Нужно скачать видео интереса: {not interest_video_exists}. "
                f"Кадры ДО: {before_channels_to_download}. "
                f"Кадры ПОСЛЕ: {after_channels_to_download}. "
                f"Итого каналы: {final_channels_to_download}"
            )

            if not final_channels_to_download:
                logger.info("Нечего скачивать, все материалы уже есть в облаке.")
                self.del_pending_interest(reg_id, interest_name)
                return None

            # 4) скачиваем по одному клипу на канал
            channels_files_dict = await cms_api.download_single_clip_per_channel(
                jsession=self.jsession,
                reg_id=reg_id,
                interest=interest,
                channels=final_channels_to_download
            )
            # оставляем полную структуру для доступа к concat_sources при отладке
            channels_info = channels_files_dict

            channels_paths = {ch: info["path"] for ch, info in channels_info.items() if info and info.get("path")}
            # 5) если надо — выгружаем «полный» клип в облако (только для chanel_id)
            full_clip_upload_status = False
            full_clip_path = None
            if not interest_video_exists:
                file_dict = channels_files_dict.get(channel_id)
                full_clip_path = file_dict["path"]
                if full_clip_path:
                     full_clip_upload_status = await self.upload_interest_video_cloud(
                        reg_id=reg_id,
                        interest_name=interest_name,
                        video_path=full_clip_path,
                        cloud_folder=cloud_paths["interest_folder_path"]
                    )
                else:
                    logger.warning(
                        f"{reg_id}: Полный клип по каналу {channel_id} не получен — пропускаем загрузку видео.")

            await cloud_uploader.aupload_dict_as_json_to_cloud(
                data=interest["report"],
                remote_folder_path=interest["cloud_folder"]
            )

            # 6) извлекаем кадры из КАЖДОГО скачанного клипа и выгружаем их
            upload_status = await self.process_frames_before_after(
                reg_id, interest, channels_paths  # ← передаём словарь!!!
            )
            ok_frames = bool(upload_status and upload_status.get("upload_status"))
            logger.info(f"Результат загрузки изображений: {ok_frames}")

            # 7) чистим локальные клипы (кроме «полного» по нужному каналу)
            removed = cms_api.delete_videos_except(
                videos_by_channel=channels_paths,
                keep_channel_id=channel_id if not interest_video_exists else None
            )
            all_done_ok = bool(ok_frames and (interest_video_exists or full_clip_upload_status))

            if full_clip_path:
                if full_clip_upload_status:
                    logger.info(
                        f"{reg_id}: Удаляем локальное видео интереса {interest_name}. ({full_clip_path}).")
                    if os.path.exists(full_clip_path):
                        os.remove(full_clip_path)
                else:
                    logger.error(f"{reg_id}: Не удалось загрузить видео интереса в {interest_name}.")
            if all_done_ok:
                if settings.config.getboolean("QT_RM", "enable_recognition"):
                    logger.info(f"{reg_id}: {interest_name} Отдаем команду на распознавание (выстерлил-забыл)")
                    asyncio.create_task(
                        self.qt_rm_client.recognize_webdav(interest_name=interest_name)
                    )
                    self.del_pending_interest(reg_id, interest_name)
                total_src_removed = 0
                for ch, info in channels_info.items():
                    sources = (info or {}).get("concat_sources") or []
                    for fp in sources:
                        try:
                            if os.path.exists(fp):
                                os.remove(fp)
                                total_src_removed += 1
                        except Exception as e:
                            logger.warning(f"{reg_id}: Не удалось удалить исходник {fp}: {e}")
                interest_temp_folder = os.path.join(settings.TEMP_FOLDER,
                                                    interest_name)
                if os.path.exists(interest_temp_folder):
                    logger.info(
                        f"{reg_id}: Удаляем временную директорию интереса {interest_name}. ({interest_temp_folder}).")
                    shutil.rmtree(interest_temp_folder)

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

    def del_pending_interest(self, reg_id, interest_name):
        try:
            main_funcs.remove_pending_interest(reg_id, interest_name)
        except Exception as e:
            logger.warning(f"{reg_id}: Не удалось удалить {interest_name} из pending_interests: {e}")


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

    async def process_frames_before_after(self, reg_id: str, enriched: dict, videos_by_channel):
        """
        ВЕРСИЯ 3 (streaming):
        1) Из каждого клипа берём первый и последний кадр как JPEG bytes (без локальных файлов)
        2) Заливаем в облако в before_pics / after_pics через PUT
        Возвращает: {"upload_status": bool}
        """
        channels = [0, 1, 2, 3]

        before_items: list[tuple[str, bytes]] = []
        after_items: list[tuple[str, bytes]] = []

        async def _extract_for_channel(ch: int, path: str | None):
            if not path:
                return None, None
            # новая функция, которая возвращает (('chX_first.jpg', bytes) | None, ('chX_last.jpg', bytes) | None)
            return await cms_api.extract_edge_frames_bytes(
                video_path=path,
                channel_id=ch,
                reg_id=reg_id,
            )

        tasks = [asyncio.create_task(_extract_for_channel(ch, videos_by_channel.get(ch))) for ch in channels]

        # Собираем результаты по мере готовности
        for ch, t in zip(channels, asyncio.as_completed(tasks)):
            first_item, last_item = await t
            if first_item:
                before_items.append(first_item)
            if last_item:
                after_items.append(last_item)

        # Загрузка без временных файлов
        ok_before = await cloud_uploader.upload_many_bytes_async(before_items, enriched["pics_before_folder"],
                                                                 content_type="image/jpeg")
        ok_after = await cloud_uploader.upload_many_bytes_async(after_items, enriched["pics_after_folder"],
                                                                content_type="image/jpeg")
        upload_status = bool(ok_before and ok_after)

        return {"upload_status": upload_status}


    async def upload_interest_video_cloud(self, reg_id, interest_name, video_path, cloud_folder):
        # Загружаем видео
        logger.info(
            f"{reg_id}: Загружаем видео интереса {interest_name} в облако.")
        upload_status = await asyncio.to_thread(
            cloud_uploader.upload_file, video_path, cloud_folder)
        return upload_status

    async def login(self):
        login_result = await cms_api.login()
        self.jsession = login_result.json()["jsession"]

    async def mainloop(self):
        logger.info("Mainloop has been launched with success.")
        self._running: set[asyncio.Task] = set()
        await self.login()

        while True:
            # важно: get_devices_online в thread, чтобы не блокировать loop
            devices_online = await self.get_devices_online()

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

    async def _refill_pending_interests_if_due(self, reg_id: str) -> None:
        """
        Пополняет очередь pending_interests для reg_id двумя способами:

        1) "Обычный" forward-проход (как раньше):
           - если сейчас > last_upload_time + 600 сек:
             - догоняем интервал [last_upload_time → now] посуточно,
               обновляя last_upload_time по мере продвижения.

        2) Новый recheck-проход по "верифицированному" интервалу:
           - берём verified_until (если его нет — используем last_upload_time),
           - если прошло >= VERIFIED_RECHECK_HOURS часов,
             ещё раз проверяем интервал [verified_until → now]
             и досовываем новые интересы в pending_interests.

        Ограничение глубины по дням (MAX_LOOKBACK_DAYS) применяется и к last_upload_time,
        и к verified_until, чтобы не уходить слишком далеко в прошлое.
        """
        if reg_id in self._interest_refill_in_progress:
            return
        self._interest_refill_in_progress.add(reg_id)
        try:
            TIME_FMT = "%Y-%m-%d %H:%M:%S"

            reg_info = main_funcs.get_reg_info(reg_id)

            # --- last_upload_time (как раньше) ---
            last_up_str = reg_info.get("last_upload_time")
            if not last_up_str:
                # как и раньше: если ничего нет — считаем 7 дней назад
                last_up_str = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime(TIME_FMT)
            last_up = datetime.datetime.strptime(last_up_str, TIME_FMT)

            now = datetime.datetime.now()

            # --- новый verified_until ---
            verified_str = reg_info.get("verified_until") or last_up_str
            try:
                verified_dt = datetime.datetime.strptime(verified_str, TIME_FMT)
            except Exception:
                logger.warning(
                    f"{reg_id}: Некорректный verified_until='{verified_str}', "
                    f"сбрасываем к last_upload_time={last_up_str}."
                )
                verified_dt = last_up

            # --- флаги "пора ли что-то делать" ---
            # forward-проход — как был: не чаще, чем раз в 600 секунд
            forward_due = (now - last_up).total_seconds() >= 600

            # recheck-проход — раз в N часов (Interests.VERIFIED_RECHECK_HOURS, по умолчанию 6)
            recheck_hours = settings.config.getint("Interests", "VERIFIED_RECHECK_HOURS", fallback=6)
            recheck_due = False
            if recheck_hours > 0:
                recheck_due = (now - verified_dt).total_seconds() >= recheck_hours * 3600

            if not forward_due and not recheck_due:
                # ни обычная догонка, ни recheck ещё не нужны
                return

            # --- ограничение глубины по дням (и для last_up, и для verified_dt) ---
            max_lookback_days = settings.config.getint("Interests", "MAX_LOOKBACK_DAYS", fallback=0)
            if max_lookback_days > 0:
                earliest_allowed = now - datetime.timedelta(days=max_lookback_days)

                if last_up < earliest_allowed:
                    logger.info(
                        f"{reg_id}: last_upload_time={last_up.strftime(TIME_FMT)} старее окна "
                        f"{max_lookback_days}d → берём не глубже {earliest_allowed.strftime(TIME_FMT)}."
                    )
                    last_up = earliest_allowed

                if verified_dt < earliest_allowed:
                    logger.info(
                        f"{reg_id}: verified_until={verified_dt.strftime(TIME_FMT)} старее окна "
                        f"{max_lookback_days}d → подрезаем до {earliest_allowed.strftime(TIME_FMT)}."
                    )
                    verified_dt = earliest_allowed

            collected: list[dict] = []

            def day_end(dt: datetime.datetime) -> datetime.datetime:
                return dt.replace(hour=23, minute=59, second=59)

            def day_start(dt: datetime.datetime) -> datetime.datetime:
                return dt.replace(hour=0, minute=0, second=0)

            # --- 1) Обычный forward-проход от last_upload_time к now ---
            if forward_due:
                cur = last_up
                today = now.date()

                # догоняем до "вчера включительно" посуточно
                while cur.date() < today:
                    st = cur.strftime(TIME_FMT)
                    en_dt = day_end(cur)
                    en = en_dt.strftime(TIME_FMT)

                    reg_cfg = main_funcs.get_reg_info(reg_id) or main_funcs.create_new_reg(reg_id, plate=None)
                    interests = await self.get_interests_async(reg_id, reg_cfg, st, en)
                    if interests:
                        interests = merge_overlapping_interests(interests)
                        collected.extend(interests)
                        # двигаем "фактический" конец до конца последнего интереса
                        en = max(interest["end_time"] for interest in interests)

                    # после закрытия дня двигаем last_upload_time до конца дня (как и раньше)
                    main_funcs.save_new_reg_last_upload_time(reg_id, en)
                    cur = en_dt + datetime.timedelta(seconds=1)

                # остаток "сегодня до текущего момента"
                if cur <= now:
                    st = cur.strftime(TIME_FMT)
                    en = now.strftime(TIME_FMT)
                    reg_cfg = main_funcs.get_reg_info(reg_id) or main_funcs.create_new_reg(reg_id, plate=None)
                    interests = await self.get_interests_async(reg_id, reg_cfg, st, en)
                    if interests:
                        interests = merge_overlapping_interests(interests)
                        collected.extend(interests)
                        en = max(interest["end_time"] for interest in interests)
                    main_funcs.save_new_reg_last_upload_time(reg_id, en)

            # --- 2) Recheck-проход от verified_until к now ---
            if recheck_due:
                st = verified_dt.strftime(TIME_FMT)
                en = now.strftime(TIME_FMT)

                logger.info(
                    f"{reg_id}: RECHECK интервала [{st} → {en}] "
                    f"после паузы {recheck_hours}ч."
                )

                reg_cfg = main_funcs.get_reg_info(reg_id) or main_funcs.create_new_reg(reg_id, plate=None)
                interests = await self.get_interests_async(reg_id, reg_cfg, st, en)
                if interests:
                    interests = merge_overlapping_interests(interests)
                    collected.extend(interests)

                # зафиксировали, что этот интервал проверен
                main_funcs.save_reg_verified_until(reg_id, en)

            # --- записываем всё, что накопили, в pending_interests (с дедупом по имени) ---
            if collected:
                main_funcs.append_pending_interests(reg_id, collected)

        finally:
            self._interest_refill_in_progress.discard(reg_id)


async def _run():
    d = Main()
    try:
        await d.mainloop()
    finally:
        # всегда освобождаем соединения httpx
        await cms_http.close_cms_async_client()


if __name__ == "__main__":
    asyncio.run(_run())
