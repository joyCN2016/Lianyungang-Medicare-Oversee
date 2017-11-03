package inca.saas.mess.lyg;

public class Schemal {

	private Integer customId;
	private String schemalName;
	private String schemalDbName;
	private String loginUrl;

	public Integer getCustomId() {
		return customId;
	}

	public void setCustomId(Integer customId) {
		this.customId = customId;
	}

	public String getSchemalName() {
		return schemalName;
	}

	public void setSchemalName(String schemalName) {
		this.schemalName = schemalName;
	}

	public String getSchemalDbName() {
		return schemalDbName;
	}

	public void setSchemalDbName(String schemalDbName) {
		this.schemalDbName = schemalDbName;
	}

	public String getLoginUrl() {
		return loginUrl;
	}

	public void setLoginUrl(String loginUrl) {
		this.loginUrl = loginUrl;
	}

	@Override
	public String toString() {
		return "Schemal [customId=" + customId + ", schemalName=" + schemalName + ", schemalDbName=" + schemalDbName
				+ ", loginUrl=" + loginUrl + "]";
	}

}
