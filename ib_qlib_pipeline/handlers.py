from __future__ import annotations

from qlib.contrib.data.handler import Alpha158


class Alpha158News(Alpha158):
    """Alpha158 with explicit daily news features."""

    _NEWS_FIELD_MAP = {
        "news_count": ("$news_count", "NEWS_COUNT0"),
        "news_sentiment": ("$news_sentiment", "NEWS_SENTIMENT0"),
        "news_negative_ratio": ("$news_negative_ratio", "NEWS_NEGATIVE_RATIO0"),
    }

    def __init__(self, *args, news_fields=None, **kwargs):
        if news_fields is None:
            self.news_fields = list(self._NEWS_FIELD_MAP.keys())
        else:
            self.news_fields = [str(x).strip().lower() for x in news_fields]
        super().__init__(*args, **kwargs)

    def get_feature_config(self):
        fields, names = super().get_feature_config()
        fields = list(fields)
        names = list(names)

        # These fields are generated into qlib bins by news_features.py.
        for key in self.news_fields:
            if key not in self._NEWS_FIELD_MAP:
                continue
            expr, col = self._NEWS_FIELD_MAP[key]
            fields.append(expr)
            names.append(col)
        return fields, names


class Alpha158Sec(Alpha158):
    """Alpha158 with explicit SEC filing-derived features."""

    _SEC_FIELD_MAP = {
        "sec_is_10k_day": ("$sec_is_10k_day", "SEC_IS_10K_DAY"),
        "sec_is_10q_day": ("$sec_is_10q_day", "SEC_IS_10Q_DAY"),
        "sec_days_since_filing": ("$sec_days_since_filing", "SEC_DAYS_SINCE_FILING"),
    }

    def __init__(self, *args, sec_fields=None, **kwargs):
        if sec_fields is None:
            self.sec_fields = list(self._SEC_FIELD_MAP.keys())
        else:
            self.sec_fields = [str(x).strip().lower() for x in sec_fields]
        super().__init__(*args, **kwargs)

    def get_feature_config(self):
        fields, names = super().get_feature_config()
        fields = list(fields)
        names = list(names)
        for key in self.sec_fields:
            if key not in self._SEC_FIELD_MAP:
                continue
            expr, col = self._SEC_FIELD_MAP[key]
            fields.append(expr)
            names.append(col)
        return fields, names
