from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException

DetectorFactory.seed = 0  # makes detection reproducible

def detect_language(text: str) -> str:
    if not text or not text.strip():
        return "unknown"
    try:
        lang = detect(text)
        return lang
    except LangDetectException:
        return "unknown"

if __name__ == "__main__":
    samples = [
        "The economy is slowing down due to inflation concerns.",
        "La economía se está desacelerando debido a la inflación.",
        "技术创新正在改变世界。",
        ""
    ]
    for s in samples:
        print(s, "→", detect_language(s))
