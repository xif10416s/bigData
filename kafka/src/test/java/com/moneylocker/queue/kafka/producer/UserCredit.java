package com.moneylocker.queue.kafka.producer;


public class UserCredit implements java.io.Serializable {

	private static final long serialVersionUID = 8824201041975101858L;

	// TODO 换Long
    private String id;

    private String userId;

    private Integer creditRevenue;

    private Integer creditExpenditure;

    private Integer creditRemaining;

	private Integer recommendCredit; // 推荐总收入

	private Integer rightCredit; // 获取积分的方式： 右滑

	private Integer leftCredit; // 获取积分的方式： 左滑

	private Integer downloadCredit; // 获取积分的方式： 下载

	private Integer exposureCredit; // 获取积分的方式：曝光，观察

	private Integer shareProductCredit; // 获取积分的方式： 分享购买的商品

	private Integer activityCredit; // 获取积分的方式： 活动获取的积分

	private Integer signCredit; // 获取积分的方式：签到积分

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Integer getCreditRevenue() {
        return creditRevenue;
    }

    public void setCreditRevenue(Integer creditRevenue) {
        this.creditRevenue = creditRevenue;
    }

    public Integer getCreditExpenditure() {
        return creditExpenditure;
    }

    public void setCreditExpenditure(Integer creditExpenditure) {
        this.creditExpenditure = creditExpenditure;
    }

    public Integer getCreditRemaining() {
        return creditRemaining;
    }

    public void setCreditRemaining(Integer creditRemaining) {
        this.creditRemaining = creditRemaining;
    }

	public Integer getRecommendCredit() {
		return recommendCredit;
	}

	public void setRecommendCredit(Integer recommendCredit) {
		this.recommendCredit = recommendCredit;
	}

	public Integer getRightCredit() {
		return rightCredit;
	}

	public void setRightCredit(Integer rightCredit) {
		this.rightCredit = rightCredit;
	}

	public Integer getLeftCredit() {
		return leftCredit;
	}

	public void setLeftCredit(Integer leftCredit) {
		this.leftCredit = leftCredit;
	}

	public Integer getDownloadCredit() {
		return downloadCredit;
	}

	public void setDownloadCredit(Integer downloadCredit) {
		this.downloadCredit = downloadCredit;
	}

	public Integer getExposureCredit() {
		return exposureCredit;
	}

	public void setExposureCredit(Integer exposureCredit) {
		this.exposureCredit = exposureCredit;
	}

	public Integer getShareProductCredit() {
		return shareProductCredit;
	}

	public void setShareProductCredit(Integer shareProductCredit) {
		this.shareProductCredit = shareProductCredit;
	}

	public Integer getActivityCredit() {
		return activityCredit;
	}

	public void setActivityCredit(Integer activityCredit) {
		this.activityCredit = activityCredit;
	}

	public Integer getSignCredit() {
		return signCredit;
	}

	public void setSignCredit(Integer signCredit) {
		this.signCredit = signCredit;
	}
}
