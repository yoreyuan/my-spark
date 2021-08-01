package yore.db;

import java.io.Serializable;

/**
 * Created by yore on 2019/3/6 15:57
 */
public class AdProvinceTopN implements Serializable {
    private String timestamp;
    private String adID;
    private String province;
    private Long clickedCount;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getAdID() {
        return adID;
    }

    public void setAdID(String adID) {
        this.adID = adID;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Long getClickedCount() {
        return clickedCount;
    }

    public void setClickedCount(Long clickedCount) {
        this.clickedCount = clickedCount;
    }
}
