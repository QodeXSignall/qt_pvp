from qt_pvp.cms_interface import functions as cms_api_funcs
from qt_pvp import functions as main_funcs
from qt_pvp.cms_interface import cms_api
from qt_pvp import cloud_uploader
from qt_pvp.logger import logger
from pathlib import Path
from qt_pvp import settings
import asyncio
import threading
import traceback
import datetime
import shutil
import time
import os


class Main:
    def __init__(self, output_format="mp4"):
        self.jsession = cms_api.login().json()["jsession"]
        #threading.Thread(target=main_funcs.video_remover_cycle).start()
        self.output_format = output_format
        self.devices_in_progress = []
        self.TIME_FMT = "%Y-%m-%d %H:%M:%S"

    def video_ready_trigger(self, *args, **kwargs):
        logger.info("Dummy trigger activated")
        pass

    def get_devices_online(self):
        devices_online = cms_api.get_online_devices(self.jsession)
        devices_online = devices_online.json()["onlines"]
        if devices_online:
            logger.debug(f"Got devices online: {devices_online}")
        return devices_online

    async def operate_device(self, reg_id):
        if reg_id in self.devices_in_progress:
            return
        self.devices_in_progress.append(reg_id)
        try:
            await self.download_reg_videos(reg_id, by_trigger=True)
        except:
            logger.error(traceback.format_exc())
        else:
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

            interests = cms_api_funcs.analyze_tracks_get_interests(
                tracks=tracks,
                by_stops=reg_info["by_stops"],
                continuous=reg_info["continuous"],
                by_lifting_limit_switch=reg_info["by_lifting_limit_switch"],
                start_tracks_search_time=start_time_dt,
                reg_id=reg_id
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

    async def download_reg_videos(self, reg_id, chanel_id: int = None,
                                  start_time=None, end_time=None,
                                  by_trigger=False, proc=False,
                                  split: int = None):
        logger.debug(f"Начинаем работу с устройством {reg_id}")
        begin_time = datetime.datetime.now()

        # Проверка доступности регистратора
        if not self.check_if_reg_online(reg_id):
            logger.info(f"{reg_id} недоступен.")
            return

        # Информация о регистраторе
        reg_info = main_funcs.get_reg_info(reg_id) or main_funcs.create_new_reg(reg_id)
        logger.debug(f"Информация о регистраторе {reg_id} - {reg_info}")
        if not reg_info:
            main_funcs.create_new_reg(reg_id)
        chanel_id = reg_info.get("chanel_id", 0)  # Если нет ID канала, ставим 0

        ignore = reg_info.get("ignore", False)
        if ignore:
            f"Игнорируем регистратор {reg_id}"
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
            return

        logger.info(f"{reg_id}: Найдено {len(interests)} интересов")
        interests = main_funcs.merge_overlapping_interests(interests)

        # Единая стратегия расширения окна — теперь задаём здесь,
        # а применяет её download_video (через download_interest_videos).
        adjustment_sequence = (0, 15, 30, 45)

        for interest in interests:
            logger.info(f"Работаем с интересом {interest}. {interests.index(interest)}/{interests}")
            interest_cloud_folder = cloud_uploader.create_interest_folder_path(
                interest_name=interest["name"],
                dest_directory=settings.CLOUD_PATH
            )

            logger.debug(f"{reg_id}: Начинаем скачивание видео для интереса {interest['name']}")
            enriched = await cms_api.download_interest_videos(
                self.jsession,
                interest,
                chanel_id,
                reg_id=reg_id,
                adjustment_sequence=adjustment_sequence,
            )

            if not enriched:
                logger.warning(f"{reg_id}: Не удалось получить видеофайлы для интереса")
                continue

            enriched["cloud_folder"] = interest_cloud_folder

            # Обработка и загрузка
            await self.process_and_upload_videos_async(reg_id, enriched)

            if settings.config.getboolean("General", "pics_before_after"):
                await self.upload_frames_before_after(reg_id, enriched)
            cloud_uploader.upload_dict_as_json_to_cloud(
                data=enriched["report"],
                remote_folder_path=enriched["cloud_folder"]
            )

        logger.info(f"{reg_id}. Все интересы обработаны.")

        # Обновляем last_upload_time
        last_interest_time = self.get_last_interest_datetime(interests) if interests else end_time
        main_funcs.save_new_reg_last_upload_time(reg_id, last_interest_time)

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
        if upload_status:
            for frame in (frames_before + frames_after):
                logger.info(f"{reg_id}: Загрузка прошла успешно. Удаляем локальные фото-файлы ({frame}).")
                os.remove(frame)

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
        output_video_path = await video_task

        if not output_video_path:
            logger.warning(
                f"{reg_id}: Нечего выгружать на облако ({output_video_path}).")
            return

        # Загружаем видео
        logger.info(
            f"{reg_id}: Загружаем видео {interest_name} в облако.")
        upload_status = await asyncio.to_thread(
            cloud_uploader.upload_file, output_video_path,
            interest["cloud_folder"]
        )

        if upload_status:
            logger.info(f"{reg_id}: Загрузка прошла успешно.")
            if settings.config.getboolean("General", "del_source_video_after_upload"):
                if os.path.exists(output_video_path):
                    logger.info(f"{reg_id}: Удаляем локальный файл ({output_video_path}).")
                    os.remove(output_video_path)
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
            await asyncio.to_thread(main_funcs.concatenate_videos,
                                    final_videos_paths_list,
                                    final_interest_video_name)
            if converted_videos and settings.config.getboolean("General", "del_source_video_after_upload"):
                logger.debug("Удаляем конвертированные файлы до конкатенации")
                for file in final_videos_paths_list:
                    if os.path.exists(file):
                        logger.debug(f"Удаляем {file}")
                        os.remove(file)
        elif len(final_videos_paths_list) == 1:
            output_video_path = final_videos_paths_list[
                0]  # Если одно видео, просто используем его
            if os.path.exists(output_video_path):
                os.rename(output_video_path, final_interest_video_name)
            else:
                logger.error(f"Ошибка при попытке использовать видео {output_video_path}. Файл не найден.")
        else:
            logger.warning(f"{reg_id}: После обработки не осталось видео.")
            return None  # Возвращаем None, если видео не обработано

        if converted_videos and settings.config.getboolean("General", "del_source_video_after_upload"):
            logger.debug("Удаляем исходные файлы до конвертации")
            for file in file_paths:
                if os.path.exists(file):
                    logger.debug(f"Удаляем {file}")
                    os.remove(file)
        return final_interest_video_name

    def get_last_interest_datetime(self, interests):
        last_interest = interests[-1]
        return last_interest["end_time"]

    async def mainloop(self):
        logger.info("Mainloop has been launched with success.")
        while True:
            devices_online = self.get_devices_online()
            for device_dict in devices_online:
                reg_id = device_dict["did"]
                await self.operate_device(reg_id)
            await asyncio.sleep(5)

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
