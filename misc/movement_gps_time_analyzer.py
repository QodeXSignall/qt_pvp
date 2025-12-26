from dataclasses import dataclass, field
from typing import List, Optional, Literal
import cv2
import numpy as np
import pytesseract
import re
import subprocess
import shutil
import os
import math


# ------------ Конфиг ------------

@dataclass
class ROI:
    x1: int
    y1: int
    x2: int
    y2: int


@dataclass
class AnalyzerConfig:
    gps_roi: ROI = field(default_factory=lambda: ROI(55, 30, 580, 60))
    time_roi: ROI = field(default_factory=lambda: ROI(55, 640, 375, 670))

    # движение
    start_motion_level = 1.0  # подобрать по логам, но 1.0 — норм старт
    stop_motion_level = 0.5  # ниже этого уже считаем, что стоим

    min_event_frames = 8  # 8 секунд устойчивого состояния

    smooth_alpha = 0.15  # сглаживание по ~4–5 секундам

    max_corners = 300
    quality_level = 0.01
    min_distance = 7
    min_tracked_points = 80  # больше точек → устойчивее

    debug_motion = True,


EventType = Literal["start_moving", "stop_moving"]


@dataclass
class MovementEvent:
    type: EventType
    frame_idx: int
    video_time_sec: float
    raw_time_text: Optional[str]
    parsed_datetime: Optional[str]
    raw_gps_text: Optional[str]


# ------------ Служебные функции ffprobe/ffmpeg ------------

def _run_command(cmd: list) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )


def prepare_video_for_opencv(input_path: str) -> str:
    """
    1) Проверяем, ffprobe/ffmpeg доступны.
    2) Узнаём кодек первого видеопотока.
    3) Если HEVC/H.265 — перекодируем в H.264 и возвращаем путь к новому файлу.
       Иначе возвращаем исходный путь.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Video file not found: {input_path}")

    ffprobe_path = shutil.which("ffprobe")
    ffmpeg_path = shutil.which("ffmpeg")

    if ffprobe_path is None or ffmpeg_path is None:
        print("[WARN] ffprobe/ffmpeg not found in PATH. "
              "Будет попытка открыть исходный файл напрямую.")
        return input_path

    print(f"[INFO] Probing codec via ffprobe: {input_path}")
    probe_cmd = [
        ffprobe_path,
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=codec_name",
        "-of", "default=nw=1:nk=1",
        input_path,
    ]
    probe_res = _run_command(probe_cmd)
    if probe_res.returncode != 0:
        print(f"[WARN] ffprobe failed: {probe_res.stderr.strip()}")
        return input_path

    codec_name = probe_res.stdout.strip()
    print(f"[INFO] Detected video codec: '{codec_name}'")

    if codec_name.lower() not in ("hevc", "h265"):
        # и так прокатывает, перекодирование не нужно
        return input_path

    # HEVC -> перекодируем
    base, ext = os.path.splitext(input_path)
    output_path = f"{base}_h264.mp4"

    print("[INFO] Transcoding HEVC -> H.264:")
    print(f"       ffmpeg -y -i {input_path} -c:v libx264 -preset veryfast -crf 23 -c:a copy {output_path}")

    transcode_cmd = [
        ffmpeg_path,
        "-y",
        "-i", input_path,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "23",
        "-c:a", "copy",
        output_path,
    ]
    trans_res = _run_command(transcode_cmd)
    if trans_res.returncode != 0:
        raise RuntimeError(f"ffmpeg transcode failed: {trans_res.stderr}")

    print("[INFO] Перекодирование завершено, используем:", output_path)
    return output_path


# ------------ Основной класс ------------

class VideoMovementAndOverlayAnalyzer:
    def __init__(self, video_path: str, config: Optional[AnalyzerConfig] = None):
        self.video_path = video_path
        self.config = config or AnalyzerConfig()

        self._prev_gray: Optional[np.ndarray] = None
        self._ema_motion: Optional[float] = None
        self._is_moving: bool = False
        self._stable_counter: int = 0

        # последнее успешно распознанное GPS
        self._last_gps_text: Optional[str] = None


    # ---- публичный метод ----

    def analyze(self) -> List[MovementEvent]:
        print(f"[INFO] Opening video with OpenCV: {self.video_path}")
        cap = cv2.VideoCapture(self.video_path)
        if not cap.isOpened():
            raise RuntimeError(f"Cannot open video: {self.video_path}")

        fps = cap.get(cv2.CAP_PROP_FPS)
        width = cap.get(cv2.CAP_PROP_FRAME_WIDTH)
        height = cap.get(cv2.CAP_PROP_FRAME_HEIGHT)
        fourcc_int = int(cap.get(cv2.CAP_PROP_FOURCC))
        fourcc = "".join([chr((fourcc_int >> 8 * i) & 0xFF) for i in range(4)])

        print(f"[INFO] OpenCV probe: FPS={fps}, size=({width}x{height}), FOURCC='{fourcc.strip()}'")

        if width == 0 or height == 0:
            cap.release()
            raise RuntimeError(
                f"Video has invalid frame size ({width}x{height}). "
                "Видимо, OpenCV всё ещё не может декодировать этот файл."
            )

        if fps <= 0 or math.isnan(fps):
            fps = 25.0

        events: List[MovementEvent] = []

        frame_idx = 0
        while True:
            ret, frame = cap.read()
            if not ret:
                if frame_idx == 0:
                    print("[WARN] Cannot read first frame. "
                          "Скорее всего, OpenCV не умеет декодировать этот кодек.")
                break

            motion_value = self._compute_motion(frame)
            if motion_value is None:
                frame_idx += 1
                continue

            prev_state = self._is_moving
            self._update_state(motion_value)

            # DEBUG: раз в 10 кадров выводим текущие значения
            if self.config.debug_motion and frame_idx % 10 == 0:
                print(
                    f"[DEBUG] frame={frame_idx}, motion={motion_value:.6f}, "
                    f"ema={self._ema_motion:.6f}, is_moving={self._is_moving}, "
                    f"stable_cnt={self._stable_counter}"
                )

            # если состояние только что сменилось и стабилизировалось N кадров — фиксируем событие
            if self._stable_counter == self.config.min_event_frames and self._is_moving != prev_state:
                event_type: EventType = "start_moving" if self._is_moving else "stop_moving"
                video_time_sec = frame_idx / fps

                raw_time, parsed_dt = self._read_time(frame)
                raw_gps = self._read_gps(frame)

                print(f"[INFO] EVENT {event_type} at frame={frame_idx}, t={video_time_sec:.2f}s,"
                      f" time='{parsed_dt}', gps='{raw_gps}'")

                events.append(
                    MovementEvent(
                        type=event_type,
                        frame_idx=frame_idx,
                        video_time_sec=video_time_sec,
                        raw_time_text=raw_time,
                        parsed_datetime=parsed_dt,
                        raw_gps_text=raw_gps,
                    )
                )

            frame_idx += 1

        cap.release()
        print(f"[INFO] Total events detected: {len(events)}")
        return events

    # ---- движение ----
    def _compute_motion(self, frame: np.ndarray) -> Optional[float]:
        """
        Возвращает величину глобального движения между кадрами (в пикселях),
        оценённую по медианному смещению фичей (goodFeaturesToTrack + LK optical flow).
        """
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

        # Маску можно сделать, чтобы отсечь самый низ, где ковыряются люди/манипулятор:
        # h, w = gray.shape[:2]
        # mask = np.zeros_like(gray, dtype=np.uint8)
        # mask[int(0.2 * h):, :] = 255   # например, брать только верх 80% кадра
        mask = None

        # Инициализация: ищем фичи на первом кадре или при потере трека
        if self._prev_gray is None or self._prev_pts is None or len(self._prev_pts) < self.config.min_tracked_points:
            self._prev_gray = gray
            self._prev_pts = cv2.goodFeaturesToTrack(
                gray,
                maxCorners=self.config.max_corners,
                qualityLevel=self.config.quality_level,
                minDistance=self.config.min_distance,
                blockSize=7,
                mask=mask,
            )
            # На самом первом шаге движения оценить нельзя
            return None

        # Считаем оптический поток (куда сместились точки)
        next_pts, status, err = cv2.calcOpticalFlowPyrLK(
            self._prev_gray,
            gray,
            self._prev_pts,
            None,
            winSize=(21, 21),
            maxLevel=3,
            criteria=(cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, 30, 0.01),
        )

        if next_pts is None or status is None:
            # что-то пошло не так — переинициализируемся на следующем кадре
            self._prev_gray = None
            self._prev_pts = None
            return None

        status = status.reshape(-1)

        good_new = next_pts[status == 1]
        good_old = self._prev_pts[status == 1]

        if good_new.size == 0 or good_old.size == 0:
            self._prev_gray = None
            self._prev_pts = None
            return None

        # приводим к форме (N, 2), убирая лишнее измерение
        good_new = good_new.reshape(-1, 2)
        good_old = good_old.reshape(-1, 2)

        if len(good_new) < self.config.min_tracked_points:
            # мало трекаемых точек, в этом кадре движение не считаем, перезапустимся
            self._prev_gray = None
            self._prev_pts = None
            return None

        # Векторы смещения
        flow = good_new - good_old  # shape (N, 2)
        dx = flow[:, 0]
        dy = flow[:, 1]

        # Берём медиану по dx/dy – это и будет глобальное смещение фона
        median_dx = float(np.median(dx))
        median_dy = float(np.median(dy))
        motion_px = float(np.hypot(median_dx, median_dy))

        # Подготовка к следующему шагу
        self._prev_gray = gray
        self._prev_pts = good_new.reshape(-1, 1, 2)

        return motion_px

    def _update_state(self, motion_value: float):
        # экспоненциальное сглаживание
        if self._ema_motion is None:
            self._ema_motion = motion_value
        else:
            a = self.config.smooth_alpha
            self._ema_motion = a * motion_value + (1 - a) * self._ema_motion

        mv = self._ema_motion

        # гистерезис: разные пороги для старта и остановки
        if self._is_moving:
            target_state = mv > self.config.stop_motion_level
        else:
            target_state = mv > self.config.start_motion_level

        if target_state == self._is_moving:
            self._stable_counter = 0
        else:
            self._stable_counter += 1
            if self._stable_counter >= self.config.min_event_frames:
                self._is_moving = target_state
                # оставляем stable_counter равным min_event_frames,
                # чтобы analyze() увидел смену состояния

    # ---- OCR GPS / Time ----

    def _crop(self, frame: np.ndarray, roi: ROI) -> np.ndarray:
        h, w = frame.shape[:2]
        x1 = max(0, min(w, roi.x1))
        x2 = max(0, min(w, roi.x2))
        y1 = max(0, min(h, roi.y1))
        y2 = max(0, min(h, roi.y2))
        if x2 <= x1 or y2 <= y1:
            # на случай некорректных ROI
            return frame[0:0, 0:0]
        return frame[y1:y2, x1:x2]

    def _preprocess_text_roi(self, img: np.ndarray) -> np.ndarray:
        if img.size == 0:
            return img
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (3, 3), 0)
        _, th = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        return th

    def _ocr(self, img: np.ndarray, whitelist: str) -> str:
        if img.size == 0:
            return ""
        proc = self._preprocess_text_roi(img)
        cfg = f'--psm 7 -c tessedit_char_whitelist="{whitelist}"'
        text = pytesseract.image_to_string(proc, config=cfg)
        return text.strip()

    def _read_gps(self, frame: np.ndarray) -> Optional[str]:
        roi_img = self._crop(frame, self.config.gps_roi)

       # Сырой OCR без лишнего мудрёжа
        raw_text = self._ocr(roi_img, "0123456789.,NSEWnsewKM/H ")
        #print(f"[DEBUG] raw OCR gps text: '{raw_text}'")

        if not raw_text or not raw_text.strip():
            print("[DEBUG] GPS OCR returned empty string")
            return None

        # Нормализуем: убираем мусор, приводим к верхнему регистру
        cleaned = raw_text.upper()
        # оставляем только допустимые символы
        cleaned = re.sub(r"[^0-9\.,NSEW]", "", cleaned)
        #print(f"[DEBUG] cleaned gps text: '{cleaned}'")

        if not cleaned:
           # print("[DEBUG] cleaned GPS text is empty after filtering")
            return None

        return cleaned

    def _read_time(self, frame: np.ndarray) -> (Optional[str], Optional[str]):
        roi_img = self._crop(frame, self.config.time_roi)

        text = self._ocr(roi_img, "0123456789-: /")  # добавим ещё / на всякий случай
        if not text:
            return None, None

        # DEBUG: посмотреть, что реально даёт tesseract
        dbg = text.replace("\n", "\\n")
        print(f"[DEBUG] OCR time raw: '{dbg}'")

        # 1) выкидываем всё, кроме цифр и разделителей
        cleaned = re.sub(r"[^\d\-: ]", " ", text)
        # 2) вытаскиваем все числа подряд
        nums = re.findall(r"\d+", cleaned)

        if len(nums) < 6:
            # не набрали год, месяц, день, час, мин, сек
            return text, None

        year, month, day, hour, minute, second = nums[:6]

        # нормализуем до двух цифр, если OCR съел ведущий 0
        month = month.zfill(2)
        day = day.zfill(2)
        hour = hour.zfill(2)
        minute = minute.zfill(2)
        second = second.zfill(2)

        parsed = f"{year}-{month}-{day} {hour}:{minute}:{second}"
        return text, parsed



# ------------ пример использования ------------

if __name__ == "__main__":
    # исходник в HEVC
    raw_video_path = "input.mp4"

    prepared_path = prepare_video_for_opencv(raw_video_path)
    analyzer = VideoMovementAndOverlayAnalyzer(prepared_path)
    analyzer.debug_motion = True
    events = analyzer.analyze()

    for e in events:
        print(
            f"{e.type.upper()} | frame={e.frame_idx} | t={e.video_time_sec:.2f}s | "
            f"time='{e.parsed_datetime}' raw_time='{e.raw_time_text}' gps='{e.raw_gps_text}'"
        )