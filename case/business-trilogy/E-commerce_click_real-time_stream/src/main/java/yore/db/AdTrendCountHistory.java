package yore.db;

import java.io.Serializable;

/**
 * Created by yore on 2019/3/6 16:00
 */
public class AdTrendCountHistory implements Serializable {
    private Long clickedCountHistory;

    public Long getClickedCountHistory() {
        return clickedCountHistory;
    }

    public void setClickedCountHistory(Long clickedCountHistory) {
        this.clickedCountHistory = clickedCountHistory;
    }
}
