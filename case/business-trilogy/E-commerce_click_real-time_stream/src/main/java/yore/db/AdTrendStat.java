package yore.db;

import java.io.Serializable;

/**
 * Created by yore on 2019/3/6 15:59
 */
public class AdTrendStat implements Serializable {
    private String adID;
    private Long clickedCount;
    private String _date;
    private String _hour;
    private String _minute;

    @Override
    public String toString() {
        return "AdTrendStat{" +
                "adID='" + adID + '\'' +
                ", clickedCount=" + clickedCount +
                ", _date='" + _date + '\'' +
                ", _hour='" + _hour + '\'' +
                ", _minute='" + _minute + '\'' +
                '}';
    }

    public String getAdID() {
        return adID;
    }

    public void setAdID(String adID) {
        this.adID = adID;
    }

    public Long getClickedCount() {
        return clickedCount;
    }

    public void setClickedCount(Long clickedCount) {
        this.clickedCount = clickedCount;
    }

    public String get_date() {
        return _date;
    }

    public void set_date(String _date) {
        this._date = _date;
    }

    public String get_hour() {
        return _hour;
    }

    public void set_hour(String _hour) {
        this._hour = _hour;
    }

    public String get_minute() {
        return _minute;
    }

    public void set_minute(String _minute) {
        this._minute = _minute;
    }
}
