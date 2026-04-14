import re
import unicodedata

FRENCH_INDICATORS = {
    "les", "des", "aux", "pour", "dans", "sur", "avec", "par",
    "est", "sont", "une", "ces", "cette", "entre", "selon",
    "comme", "mais", "aussi", "donc", "leurs",
}


def normalize_header(raw: str) -> str:
    """Reduce meaningless variation in a raw header string.

    Steps:
    - Strip leading/trailing whitespace
    - Collapse internal whitespace and newlines to single space
    - Lowercase
    - Normalize Unicode dashes (en-dash, em-dash) to hyphen
    - Normalize smart quotes to plain quotes
    - Remove BOM characters
    - Standardize % and $ symbols
    - Strip trailing colons
    """
    if not raw:
        return ""

    text = raw

    # Remove BOM
    text = text.replace("\ufeff", "")

    # Collapse whitespace and newlines
    text = re.sub(r"[\s\r\n]+", " ", text).strip()

    # Lowercase
    text = text.lower()

    # Normalize Unicode dashes to hyphen
    text = text.replace("\u2013", "-")  # en-dash
    text = text.replace("\u2014", "-")  # em-dash
    text = text.replace("\u2012", "-")  # figure dash
    text = text.replace("\u2015", "-")  # horizontal bar

    # Normalize smart quotes
    text = text.replace("\u2018", "'").replace("\u2019", "'")
    text = text.replace("\u201c", '"').replace("\u201d", '"')

    # Standardize percentage notation
    text = re.sub(r"\(\s*%\s*\)", "(%)", text)

    # Standardize dollar notation
    text = re.sub(r"m\$", "millions $", text)
    text = re.sub(r"\$us", "$ us", text)

    # Strip trailing colons
    text = text.rstrip(":")

    # Final whitespace collapse
    text = re.sub(r"\s+", " ", text).strip()

    return text


def detect_language(text: str) -> str:
    """Simple heuristic language detection.

    Returns 'en', 'fr', or 'bilingual'.
    """
    if not text:
        return "unknown"

    lower = text.lower()
    words = set(re.findall(r"[a-zàâçéèêëîïôùûüÿæœ]+", lower))

    has_french_chars = bool(re.search(r"[àâçéèêëîïôùûüÿæœ]", lower))
    french_word_count = len(words & FRENCH_INDICATORS)

    if has_french_chars or french_word_count >= 2:
        # Check if it also has English indicators
        english_indicators = {"the", "and", "for", "are", "with", "from", "that", "this"}
        english_word_count = len(words & english_indicators)
        if english_word_count >= 2:
            return "bilingual"
        return "fr"

    return "en"
