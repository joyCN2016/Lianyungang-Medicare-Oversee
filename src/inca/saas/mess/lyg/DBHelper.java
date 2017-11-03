package inca.saas.mess.lyg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Category;

public class DBHelper {

	static Category log = Category.getInstance(DBHelper.class);
	public static final String name = "org.postgresql.Driver";

	public static final String dboneUrl = "jdbc:postgresql://10.210.4.33:5432/lygsaas";
	public static final String dboneUser = "postgres";
	public static final String dbonePassword = "incainca";
	public static Connection dboneConn;

	public static final String dbtwoUrl = "jdbc:postgresql://10.210.4.34:5432/lygsaas";
	public static final String dbtwoUser = "postgres";
	public static final String dbtwoPassword = "incainca";
	public static Connection dbtwoConn;
	public static String softName = "";

//  上海服务器跳板机
	public static final String dbgyyUrl = "jdbc:postgresql://10.105.18.254:5432/saas";
	public static final String dbgyyUser = "saas";
	public static final String dbgyyPassword = "IncaSaasDB";
	public static Connection dbgyyConn;
//	public static final String oracleUrl = "jdbc:oracle:thin:@61.177.174.9:12303/orcl";
	
	// 连接连云港跳板，再连接远程时oracle数据库路径
	public static final String oracleUrl = "jdbc:oracle:thin:@localhost:1521/orcl";
	public static final String oracleName = "oracle.jdbc.driver.OracleDriver";
	public static final String oracleUser = "system";
	public static final String oraclePassword = "IncaSaasDB2016";

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		log.info("上传开始时间：" + start);
		List<Schemal> schemalList = getSchemalList();
		List<Schemal> dboneList = new ArrayList<>();
		List<Schemal> dbtwoList = new ArrayList<>();
		List<Schemal> dbgyyList = new ArrayList<>();
		for (Schemal schemal : schemalList) {
			String schemalDbName = schemal.getSchemalDbName();
			if (schemalDbName.equals("db1")) {
				dboneList.add(schemal);
			} else if  (schemalDbName.equals("db2")) {
				dbtwoList.add(schemal);
			} else{
				dbgyyList.add(schemal);
			}
		}
		log.info("dboneList : " + dboneList.size());
		log.info("dbtwoList : " + dbtwoList.size());
		log.info("dbgyyList : " + dbgyyList.size());
		log.info("开始上传DB1数据---start");
		start(dboneUrl, dboneUser, dbonePassword, dboneList);
		log.info("开始DB1数据完成---end");
		log.info("开始上传DB2数据---start");
		start(dbtwoUrl, dbtwoUser, dbtwoPassword, dbtwoList);
		log.info("上传DB2数据完成---end");
		log.info("开始上传上海服务器公有云数据---start");
		start(dbgyyUrl, dbgyyUser, dbgyyPassword, dbgyyList);
		log.info("上传上海服务器公有云数据完成---end");
		long end = System.currentTimeMillis();
		log.info("上传结束时间：" + end);
		log.info("上传执行完成，持续时间：" + (end - start) / 1000 + " s");
	}

	private static void start(String pgurl, String pguser, String pgpsw, List<Schemal> dbList) {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet resultSet = null;
		Connection orclCon = null;
		PreparedStatement orclPst = null;
		ResultSet orclSet = null;
		int retailCount = 0;
		for (Schemal schemal : dbList) {
			retailCount++;
			String loginUrl = schemal.getLoginUrl();
			String schemalName = schemal.getSchemalName();
			log.info("当前第" + retailCount + "个用户信息 loginUrl:" + loginUrl + " schemalName: " + schemalName);
			try {
				Class.forName(name);
				Class.forName(oracleName);
				conn = DriverManager.getConnection(pgurl, pguser, pgpsw);// 获取连接
				orclCon = DriverManager.getConnection(oracleUrl, oracleUser, oraclePassword);// 获取连接
				process(conn, pst, resultSet, orclCon, orclPst, orclSet, dbList, schemal);
			} catch (Exception e) {
				log.info("当前第" + retailCount + "个用户信息 loginUrl:" + loginUrl + " schemalName: " + schemalName 
							+ "===最外部异常:" + e.getMessage());
				e.printStackTrace();
				rollbackCon(orclCon);
				rollbackCon(conn);
			} finally {
				closeResuSet(resultSet);
				closeStatement(pst);
				closeConn(conn);
				closeResuSet(orclSet);
				closeStatement(orclPst);
				closeConn(orclCon);
			}
		}

	}

	private static void process(Connection conn, PreparedStatement pst, ResultSet query, Connection orclConn,
			PreparedStatement orclPst, ResultSet orclQuery, List<Schemal> dblist, Schemal schemal) throws Exception {
		System.gc();
		Integer customId = schemal.getCustomId();
		String loginUrl = schemal.getLoginUrl();
		String schemalName = schemal.getSchemalName();
		log.info("当前用户信息 loginUrl:" + loginUrl + " schemalName: " + schemalName);
		// ---------------------把起初库存、出入库标记设置为false
		log.info("把起初库存、出入库标记设置为false--start");
		SetDefaultFalg(conn, pst, schemalName);
		log.info("把起初库存、出入库标记设置为false--end");
		
		
		log.info("判断是否为第三方软件商，则先根据历史业务数据生成对应 出入库单----start");
//		if(true)return;
		if(getIsThird(conn, pst, schemalName) == 1){
//			ThirtyHelper.process(conn, pst, query, dblist, schemal);
			return;
		}
		log.info("判断是否为第三方软件商，则先根据历史业务数据生成对应 出入库单----end");
		
		String insertOracleStio = "";
		// --------------------------期初
		log.info("查询期初库存数据--start");
		String iniSql = "SELECT stio.id,stio.create_dept_id, stio.goods_id, SUM (dtl.goods_qty) AS goods_qty, goods.goods_name, goods.goods_code as erp_goods_code,"
				+" goods.rx_flag, goods.goods_spec, goods.goods_type, goods.goods_unit, retail.ybshopcode, "
				+ "retail.retail_name, ybgoods.yb_goods_code FROM " + schemalName 
				+ ".war_stio_doc stio left join " + schemalName 
				+ ".war_stio_dtl dtl on stio.id = dtl.doc_id  "
				+ "left join " + schemalName 
				+ ".pub_goods goods on stio.goods_id = goods.id  "
				+ "LEFT JOIN " + schemalName 
				+ ".yb_goods ybgoods ON stio.goods_id = ybgoods.goods_id "
				+ "LEFT JOIN " + schemalName 
				+ ".pub_dept dept ON stio.create_dept_id = dept. ID "
				+ "LEFT JOIN " + schemalName 
				+ ".pub_retail retail ON retail. ID = dept. ID "
				+ "WHERE stio.source_entity = 'InitStqtyLst' AND stio.create_dept_id = retail. ID "
				+ "AND stio.goods_id IN ( SELECT goods_id FROM " + schemalName 
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' "
				+ "AND ( yb_goods_code IS NOT NULL AND yb_goods_code != '' ) ) "
				+ "AND stio.create_dept_id IN ( SELECT A .retail_id FROM " + schemalName 
				+ ".lybyb_retail A "
				+ "LEFT JOIN " + schemalName 
				+ ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ "A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '' ) "
				+ "AND NOT EXISTS ( SELECT 1 FROM " + schemalName 
				+ ".sys_hide_goods WHERE goods_id = stio.goods_id ) "
				+ " AND stio .status = 1 " + " and (stio.yb_complete_flag is not true)"
				+ "GROUP BY stio.id,ybgoods.yb_goods_code, goods.goods_name, goods.rx_flag, goods.goods_spec, goods.goods_type, "
				+ "goods.goods_unit, retail.ybshopcode, retail.retail_name, stio.goods_id, stio.create_dept_id, goods.goods_code";
		log.info("查询期初库存数据iniSql: " + iniSql);
		pst = conn.prepareStatement(iniSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询期初库存数据获取结果集: " + query);
		
		insertOracleStio = insertOracleIni(orclConn, orclPst, query);
		log.info("添加期初库存数据完成");
		log.info("修改期初标记----start");
		updateOracleIni(conn, pst, insertOracleStio, schemalName);
		log.info("修改期初标记----end");
		
		if(true){
			return;
		}
		
		// -------------移库调拨单(配送)
		log.info("查询移库调拨单(配送)数据--start");
		String transferSql = " SELECT doc .busi_date , stio .come_from, stio .source_id, stio.create_dept_id, stio. ID, stio.inout_flag, retail .retail_name , stio.goods_id, "
				+ " doc.document_code,ybgoods.yb_goods_code,goods.goods_name,dtl.goods_qty,goods.rx_flag,goods.goods_code as erp_goods_code,"
				+ " goods.goods_spec,goods.goods_type,goods.goods_unit, retail.ybshopcode,supply.supply_name,"
				+ " lot.prod_date,lot.invalid_date,lot.lot_no,dtl.unit_price,dtl.amount_money "

		+ " FROM " + schemalName + ".war_stio_doc stio " + " left join " + schemalName
				+ ".war_transfer_dtl dtl on stio.source_id = dtl.id " + " left join " + schemalName
				+ ".war_transfer_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on stio.goods_id = goods.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on goods.id = ybgoods.goods_id " + " left join " + schemalName
				+ ".pub_supply supply on goods.def_supply_id = supply.id " + " left join " + schemalName
				+ ".pub_lot lot on dtl.lot_id =lot.id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON stio .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE stio .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND stio .create_dept_id IN " + " (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = stio .goods_id) " + " AND stio .retail_flag = TRUE "
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " AND stio .status = 1 " + " and (stio.yb_complete_flag is not true)"
				+ "  and stio.inout_flag = 1 and stio.come_from like 'StTransfer'  "
				+ " ORDER BY doc .busi_date ASC";
		log.info("查询移库调拨单(配送)数据transferSql: " + transferSql);
		pst = conn.prepareStatement(transferSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询移库调拨单(配送)数据获取结果集transQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		insertOracleStio = insertOracleStio(orclConn, orclPst, query, customId, loginUrl, 3);
		log.info("添加移库调拨单(配送)数据完成");
		log.info("修改移库调拨单(配送)标记--start");
		updateOracleStio(conn, pst, insertOracleStio, schemalName);
		log.info("修改移库调拨单(配送)标记--end");

		// -------------移库调拨单(配退)
		log.info("查询移库调拨单(配退)数据--start");
		String transferBackSql = " SELECT doc .busi_date , stio .come_from, stio .source_id, stio.create_dept_id, stio. ID, stio.inout_flag, retail .retail_name , stio.goods_id, "
				+ " doc.document_code,ybgoods.yb_goods_code,goods.goods_code as erp_goods_code,goods.goods_name,dtl.goods_qty,goods.rx_flag,"
				+ " goods.goods_spec,goods.goods_type,goods.goods_unit, retail.ybshopcode,supply.supply_name,"
				+ " lot.prod_date,lot.invalid_date,lot.lot_no,dtl.unit_price,dtl.amount_money "

		+ " FROM " + schemalName + ".war_stio_doc stio " + " left join " + schemalName
				+ ".dis_gpcs_back_dtl dtl on stio.source_id = dtl.id " + " left join " + schemalName
				+ ".dis_gpcs_back_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on stio.goods_id = goods.id " + " left join " + schemalName
				+ ".pub_supply supply on goods.def_supply_id = supply.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on goods.id = ybgoods.goods_id " + " left join " + schemalName
				+ ".pub_lot lot on dtl.lot_id =lot.id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON stio .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE stio .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND stio .create_dept_id IN " + " (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = stio .goods_id) " + " AND stio .retail_flag = TRUE "
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " AND stio .status = 1 " + " and (stio.yb_complete_flag is not true)"
				+ " and stio.inout_flag != 1 and stio.come_from like 'StTransfer' "
				+ " ORDER BY doc .busi_date ASC";
		log.info("查询移库调拨单(配退)数据transferBackSql: " + transferBackSql);
		pst = conn.prepareStatement(transferBackSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询移库调拨单(配退)数据获取结果集transferBackQuery: " + query + " customId :" + customId + " loginUrl :"
				+ loginUrl);
		String transferBack = insertOracleStio(orclConn, orclPst, query, customId, loginUrl, 4);
		log.info("添加移库调拨单(配退)数据完成");
		log.info("修改移库调拨单(配退)标记--start");
		updateOracleStio(conn, pst, transferBack, schemalName);
		log.info("修改移库调拨单(配退)标记--end");

		// ---------------采退(配退)
		log.info("查询采退(配退)数据--start");
		String purchasebackDelSql = " SELECT doc.busi_date , stio.come_from, stio.source_id, stio.create_dept_id, stio.ID, stio.inout_flag, "
				+ " retail .retail_name , retail.retail_code, supply.supply_name, supply.self_flag,"
				+ " doc.document_code, retail.ybshopcode, goods.goods_code as erp_goods_code,"
				+ " ybgoods.yb_goods_code,goods.goods_name,goods.rx_flag,goods.goods_spec,goods.goods_type,goods.goods_unit, "
				+ " lot.prod_date,lot.invalid_date,lot.lot_no, " + " dtl.goods_qty,dtl.unit_price,dtl.amount_money "

		+ " FROM " + schemalName + ".war_stio_doc stio " + " left join " + schemalName
				+ ".pur_purchase_dtl dtl on stio.source_id = dtl.id " + " left join " + schemalName
				+ ".pur_purchase_doc doc on dtl.doc_id = doc.id " + "	left join " + schemalName
				+ ".pub_goods goods on stio.goods_id = goods.id " + " left join " + schemalName
				+ ".pub_supply supply on goods.def_supply_id = supply.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on goods.id = ybgoods.goods_id " + " left join " + schemalName
				+ ".pub_lot lot on dtl.lot_id = lot.id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON stio .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE stio .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND stio .create_dept_id IN (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = stio.goods_id) " + " AND stio .retail_flag = TRUE "
				+ " AND stio .status = 1 " + " and (stio.yb_complete_flag is not true)"
				+ " and stio.come_from like 'SuBack' " + " and supply.self_flag is true "
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " ORDER BY doc .busi_date ASC";
		log.info("查询采退(配退)数据purchasebackDelSql: " + purchasebackDelSql);
		pst = conn.prepareStatement(purchasebackDelSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询移库调拨单(配退)数据获取结果集purchasebackDelQuery: " + query + " customId :" + customId + " loginUrl :"
				+ loginUrl);
		String purchasebackDel = insertOracleStio(orclConn, orclPst, query, customId, loginUrl, 4);
		log.info("添加采退(配退)数据完成");
		log.info("修改采退(配退)标记--start");
		updateOracleStio(conn, pst, purchasebackDel, schemalName);
		log.info("修改采退(配退)标记--end");

		// ---------------采退
		log.info("查询采退数据--start");
		String purchasebackSql = " SELECT doc.busi_date , stio.come_from, stio.source_id, stio.create_dept_id, stio.ID, stio.inout_flag, "
				+ " retail .retail_name , retail.retail_code, supply.supply_name, supply.self_flag,"
				+ " doc.document_code, retail.ybshopcode, goods.goods_code as erp_goods_code,"
				+ " ybgoods.yb_goods_code,goods.goods_name,goods.rx_flag,goods.goods_spec,goods.goods_type,goods.goods_unit, "
				+ " lot.prod_date,lot.invalid_date,lot.lot_no, " + " dtl.goods_qty,dtl.unit_price,dtl.amount_money "

		+ " FROM " + schemalName + ".war_stio_doc stio " + " left join " + schemalName
				+ ".pur_purchase_dtl dtl on stio.source_id = dtl.id " + " left join " + schemalName
				+ ".pur_purchase_doc doc on dtl.doc_id = doc.id " + "	left join " + schemalName
				+ ".pub_goods goods on stio.goods_id = goods.id " + " left join " + schemalName
				+ ".pub_supply supply on goods.def_supply_id = supply.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on goods.id = ybgoods.goods_id " + " left join " + schemalName
				+ ".pub_lot lot on dtl.lot_id = lot.id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON stio .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE stio .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND stio .create_dept_id IN (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = stio.goods_id) " + " AND stio .retail_flag = TRUE "
				+ " AND stio .status = 1 " + " and (stio.yb_complete_flag is not true)"
				+ " and stio.come_from like 'SuBack' " + " and supply.self_flag is not true "
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " ORDER BY doc .busi_date ASC";
		log.info("查询采退数据purchasebackSql: " + purchasebackSql);
		pst = conn.prepareStatement(purchasebackSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询采退数据获取结果集purchasebackQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String purchaseback = insertOracleStio(orclConn, orclPst, query, customId, loginUrl, 10);
		log.info("添加采退数据完成");
		log.info("修改采退标记--start");
		updateOracleStio(conn, pst, purchaseback, schemalName);
		log.info("修改采退标记--end");

		// -----------零退
		log.info("查询零退数据--start");
		String resaBackSql = " SELECT doc .busi_date , stio .come_from, stio .source_id, stio.create_dept_id, stio. ID, stio.inout_flag, stio.goods_id, "
				+ " doc.document_code, supply.supply_name, retail .retail_name , retail.retail_code , retail.ybshopcode,"
				+ " ybgoods.yb_goods_code,goods.goods_name,dtl.goods_qty,goods.rx_flag,"
				+ " goods.goods_spec,goods.goods_type,goods.goods_unit,goods.goods_code,goods.goods_code as erp_goods_code,"
				+ " lot.prod_date,lot.invalid_date,lot.lot_no," + " dtl.unit_price,dtl.real_money as amount_money,"
				+ " doc.yb_pay_money,doc.yb_card_no,doc.yb_user_name, "
				+ " doc.user_code,doc.yb_trade_no,doc.yb_tc_money,doc.yb_sb_money,doc.yb_resa_sex,doc.yb_person_type,doc.yb_pay_money,"
				+ " doc.yb_pay_flag, doc.medical_insurance_no, "
				+ " doc.yb_own_pay,doc.yb_own_expense,doc.yb_invoice_no,doc.yb_id_card,doc.yb_gwy_money,doc.yb_db_money,doc.yb_cycle_no, "
				+ " doc.yb_area,doc.yb_cash_money,doc.resa_type,doc.id,dtl.id, "
				+ " (case when 1=1 then 0 else 1 end ) software_name, "
				+ " (case when 1=1 then 0 else 1 end ) bill_type,dtl.resa_price, dtl.pack_qty,dtl.pack_name   "

		+ " FROM " + schemalName + ".war_stio_doc stio " + " left join " + schemalName
				+ ".rsa_resa_dtl dtl on stio.source_id = dtl.id " + " left join " + schemalName
				+ ".rsa_resa_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on stio.goods_id = goods.id " + " left join " + schemalName
				+ ".pub_supply supply on goods.def_supply_id = supply.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on dtl.goods_id = ybgoods.goods_id " + " left join " + schemalName
				+ ".pub_lot lot on dtl.lot_id = lot.id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON stio .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE stio .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND stio .create_dept_id IN (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = stio .goods_id) " + " AND stio .retail_flag = TRUE "
				+ " AND stio .status = 1 " + " and (stio.yb_complete_flag is not true)"
				+ " and stio.come_from like 'ResaDoc' and doc.resa_type = 2" 
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " ORDER BY doc .busi_date ASC";
		log.info("查询零退数据resaBackSql: " + resaBackSql);
		pst = conn.prepareStatement(resaBackSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询零退数据获取结果集resaBackQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String resaBack = insertOracleStio(orclConn, orclPst, query, customId, loginUrl, 2);
		log.info("添加零退数据完成");
		log.info("修改零退标记--start");
		updateOracleStio(conn, pst, resaBack, schemalName);
		log.info("修改零退标记--end");

		// -----------零售
		log.info("查询零售数据--start");
		if(!"lygwqt2295".equals(schemalName)){
			String resaSql = " SELECT doc .busi_date , stio .come_from, stio .source_id, stio.create_dept_id, stio. ID, stio.inout_flag, stio.goods_id, "
					+ " doc.document_code, supply.supply_name, retail .retail_name , retail.retail_code , retail.ybshopcode,"
					+ " ybgoods.yb_goods_code,goods.goods_name,dtl.goods_qty,goods.rx_flag,"
					+ " goods.goods_spec,goods.goods_type,goods.goods_unit,goods.goods_code,goods.goods_code as erp_goods_code,"
					+ " lot.prod_date,lot.invalid_date,lot.lot_no," + " dtl.unit_price,dtl.real_money as amount_money,"
					+ " doc.yb_pay_money,doc.yb_card_no,doc.yb_user_name, doc.medical_insurance_no,"
					+ " doc.user_code,doc.yb_trade_no,doc.yb_tc_money,doc.yb_sb_money,doc.yb_resa_sex,doc.yb_person_type,doc.yb_pay_money,"
					+ " doc.yb_pay_flag, "
					+ " doc.yb_own_pay,doc.yb_own_expense,doc.yb_invoice_no,doc.yb_id_card,doc.yb_gwy_money,doc.yb_db_money,doc.yb_cycle_no, "
					+ " doc.yb_area,doc.yb_cash_money,doc.resa_type,doc.id,dtl.id, "
					+ " (case when 1=1 then 0 else 1 end ) software_name, "
					+ " (case when 1=1 then 0 else 1 end ) bill_type,dtl.resa_price, dtl.pack_qty,dtl.pack_name "

			+ " FROM " + schemalName + ".war_stio_doc stio " + " left join " + schemalName
					+ ".rsa_resa_dtl dtl on stio.source_id = dtl.id " + " left join " + schemalName
					+ ".rsa_resa_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
					+ ".pub_goods goods on stio.goods_id = goods.id " + " left join " + schemalName
					+ ".yb_goods ybgoods on dtl.goods_id = ybgoods.goods_id " + " left join " + schemalName
					+ ".pub_supply supply on goods.def_supply_id = supply.id " + " left join " + schemalName
					+ ".pub_lot lot on dtl.lot_id = lot.id " + " LEFT JOIN " + schemalName
					+ ".lybyb_retail ybretail ON stio .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
					+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

			+ " WHERE stio .goods_id IN " + " (SELECT goods_id FROM " + schemalName
					+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
					+ " AND stio .create_dept_id IN (SELECT A .retail_id FROM " + schemalName
					+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
					+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
					+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
					+ ".sys_hide_goods WHERE goods_id = stio .goods_id) " + " AND stio .retail_flag = TRUE "
					+ " AND stio .status = 1 " + " and (stio.yb_complete_flag is not true)"
					+ " and stio.come_from like 'ResaDoc'  and doc.resa_type = 1"
					+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
					+ " ORDER BY doc .busi_date ASC ";
			log.info("查询零售数据resaSql: " + resaSql);
			pst = conn.prepareStatement(resaSql);// 准备执行语句
			query = pst.executeQuery();
			log.info("查询零售数据获取结果集resaQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
			String resa = insertOracleStio(orclConn, orclPst, query, customId, loginUrl, 1);
			log.info("添加零售数据完成");
			log.info("修改零售标记--start");
			updateOracleStio(conn, pst, resa, schemalName);
			log.info("修改零售标记--end");
		}

		// -----------零售  2295lygwqt 这家店数据有三十多万，需要循环操作  i的值自己设定  lygwqt2295
//			/*log.info("查询零售数据--start");
		if("lygwqt2295".equals(schemalName)){
			log.info("查询零售数据--start--零售  2295lygwqt 这家店数据有三十六万多，需要循环操作  i的值自己设定  lygwqt2295");
			int count = 0;
			for (int i = 0; i < 20; i++) {

				String resaSql = " SELECT doc .busi_date , stio .come_from, stio .source_id, stio.create_dept_id, stio. ID, stio.inout_flag, stio.goods_id, "
						+ " doc.document_code, supply.supply_name, retail .retail_name , retail.retail_code , retail.ybshopcode,"
						+ " ybgoods.yb_goods_code,goods.goods_name,dtl.goods_qty,goods.rx_flag,"
						+ " goods.goods_spec,goods.goods_type,goods.goods_unit,goods.goods_code,goods.goods_code as erp_goods_code,"
						+ " lot.prod_date,lot.invalid_date,lot.lot_no," + " dtl.unit_price,dtl.real_money as amount_money,"
						+ " doc.yb_pay_money,doc.yb_card_no,doc.yb_user_name, doc.medical_insurance_no,"
						+ " doc.user_code,doc.yb_trade_no,doc.yb_tc_money,doc.yb_sb_money,doc.yb_resa_sex,doc.yb_person_type,doc.yb_pay_money,"
						+ " doc.yb_pay_flag, "
						+ " doc.yb_own_pay,doc.yb_own_expense,doc.yb_invoice_no,doc.yb_id_card,doc.yb_gwy_money,doc.yb_db_money,doc.yb_cycle_no, "
						+ " doc.yb_area,doc.yb_cash_money,doc.resa_type,doc.id,dtl.id, "
						+ " (case when 1=1 then 0 else 1 end ) software_name, "
						+ " (case when 1=1 then 0 else 1 end ) bill_type,dtl.resa_price, dtl.pack_qty,dtl.pack_qty,dtl.pack_name  "

				+ " FROM " + schemalName + ".war_stio_doc stio " + " left join " + schemalName
						+ ".rsa_resa_dtl dtl on stio.source_id = dtl.id " + " left join " + schemalName
						+ ".rsa_resa_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
						+ ".pub_goods goods on stio.goods_id = goods.id " + " left join " + schemalName
						+ ".yb_goods ybgoods on dtl.goods_id = ybgoods.goods_id " + " left join " + schemalName
						+ ".pub_supply supply on goods.def_supply_id = supply.id " + " left join " + schemalName
						+ ".pub_lot lot on dtl.lot_id = lot.id " + " LEFT JOIN " + schemalName
						+ ".lybyb_retail ybretail ON stio .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
						+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

				+ " WHERE stio .goods_id IN " + " (SELECT goods_id FROM " + schemalName
						+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
						+ " AND stio .create_dept_id IN (SELECT A .retail_id FROM " + schemalName
						+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
						+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
						+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
						+ ".sys_hide_goods WHERE goods_id = stio .goods_id) " + " AND stio .retail_flag = TRUE "
						+ " AND stio .status = 1 " + " and (stio.yb_complete_flag is not true)"
						+ " and stio.come_from like 'ResaDoc'  and doc.resa_type = 1"
						+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
						+ " ORDER BY doc .busi_date ASC limit 20000 offset " + count;
				log.info("查询零售数据resaSql: " + resaSql);
				pst = conn.prepareStatement(resaSql);// 准备执行语句
				query = pst.executeQuery();
				log.info("查询零售数据获取结果集resaQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
				String resa = insertOracleStio(orclConn, orclPst, query, customId, loginUrl, 1);
				log.info("添加零售数据完成");
				log.info("修改零售标记--start");
				updateOracleStio(conn, pst, resa, schemalName);
				log.info("修改零售标记--end");
				count += 20000;
			}
		}
		
		// --------------采购(配送)
		log.info("查询采购(配送)数据--start");
		String purchaseDelSql = " SELECT doc .busi_date , stio .come_from, stio .source_id, stio.create_dept_id, stio. ID, stio.inout_flag, "
				+ " retail .retail_name , stio.goods_id, " + " doc.document_code, retail.ybshopcode,goods.goods_code as erp_goods_code,"
				+ " ybgoods.yb_goods_code,goods.goods_name,goods.rx_flag,goods.goods_spec,goods.goods_type,goods.goods_unit,"
				+ " lot.prod_date,lot.invalid_date,lot.lot_no, "
				+ " dtl.unit_price,dtl.amount_money,dtl.goods_qty, "
				+ " supply.self_flag, supply.supply_name, supply.self_flag "

		+ " FROM " + schemalName + ".war_stio_doc stio " + " left join " + schemalName
				+ ".pur_purchase_dtl dtl on stio.source_id = dtl.id " + " left join " + schemalName
				+ ".pur_purchase_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on stio.goods_id = goods.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on dtl.goods_id = ybgoods.goods_id " + " left join " + schemalName
				+ ".pub_lot lot on dtl.lot_id = lot.id " + " left join " + schemalName
				+ ".pub_supply supply on doc.supply_id = supply.id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON stio .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE stio .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND stio .create_dept_id IN " + " (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = stio.goods_id) " + " AND stio .retail_flag = TRUE "
				+ " AND stio .status = 1 " + " and (stio.yb_complete_flag is not true)"
				+ " and stio.come_from like 'SuPur' " + " and supply.self_flag is true "
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " ORDER BY doc .busi_date ASC";
		log.info("查询采购(配送)数据purchaseDelSql: " + purchaseDelSql);
		pst = conn.prepareStatement(purchaseDelSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询采购(配送)数据获取结果集purchaseDelQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String purchaseDel = insertOracleStio(orclConn, orclPst, query, customId, loginUrl, 3);
		log.info("添加采购(配送)数据完成");
		log.info("修改采购(配送)标记--start");
		updateOracleStio(conn, pst, purchaseDel, schemalName);
		log.info("修改采购(配送)标记--end");

		// --------------采购(配送)
		log.info("查询采购(配送)数据--start");
		String purchaseSql = " SELECT doc .busi_date , stio .come_from, stio .source_id, stio.create_dept_id, stio. ID, stio.inout_flag, "
				+ " retail .retail_name , stio.goods_id, " + " doc.document_code, retail.ybshopcode,"
				+ " ybgoods.yb_goods_code,goods.goods_name,goods.rx_flag,goods.goods_spec,goods.goods_type,goods.goods_unit,"
				+ " lot.prod_date,lot.invalid_date,lot.lot_no, goods.goods_code as erp_goods_code,"
				+ " dtl.unit_price,dtl.amount_money,dtl.goods_qty, "
				+ " supply.self_flag, supply.supply_name, supply.self_flag "

		+ " FROM " + schemalName + ".war_stio_doc stio " + " left join " + schemalName
				+ ".pur_purchase_dtl dtl on stio.source_id = dtl.id " + " left join " + schemalName
				+ ".pur_purchase_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on stio.goods_id = goods.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on dtl.goods_id = ybgoods.goods_id " + " left join " + schemalName
				+ ".pub_lot lot on dtl.lot_id = lot.id " + " left join " + schemalName
				+ ".pub_supply supply on doc.supply_id = supply.id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON stio .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE stio .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND stio .create_dept_id IN " + " (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = stio.goods_id) " + " AND stio .retail_flag = TRUE "
				+ " AND stio .status = 1 " + " and (stio.yb_complete_flag is not true)"
				+ " and stio.come_from like 'SuPur' " + " and supply.self_flag is not true "
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " ORDER BY doc .busi_date ASC";
		log.info("查询采购(配送)数据purchaseSql: " + purchaseSql);
		pst = conn.prepareStatement(purchaseSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询采购(配送)数据获取结果集purchaseQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String purchase = insertOracleStio(orclConn, orclPst, query, customId, loginUrl, 9);
		log.info("添加采购(配送)数据完成");
		log.info("修改采购(配送)标记--start");
		updateOracleStio(conn, pst, purchase, schemalName);
		log.info("修改采购(配送)标记--end");

		// --------------配退
		log.info("查询配退数据--start");
		String gpcsbackSql = " SELECT doc.busi_date , stio.come_from, stio.source_id, stio.create_dept_id, stio.ID, stio.inout_flag, "
				+ " retail .retail_name , retail.retail_code, supply.supply_name, "
				+ " doc.document_code, retail.ybshopcode,goods.goods_code as erp_goods_code,"
				+ " ybgoods.yb_goods_code,goods.goods_name,goods.rx_flag,goods.goods_spec,goods.goods_type,goods.goods_unit, "
				+ " lot.prod_date,lot.invalid_date,lot.lot_no, " + " dtl.goods_qty,dtl.unit_price,dtl.amount_money "

		+ " FROM " + schemalName + ".war_stio_doc stio " + " left join " + schemalName
				+ ".dis_gpcs_back_dtl dtl on stio.source_id = dtl.id " + " left join " + schemalName
				+ ".dis_gpcs_back_doc doc on dtl.doc_id = doc.id " + "	left join " + schemalName
				+ ".pub_goods goods on stio.goods_id = goods.id " + " left join " + schemalName
				+ ".pub_supply supply on goods.def_supply_id = supply.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on goods.id = ybgoods.goods_id " + " left join " + schemalName
				+ ".pub_lot lot on dtl.lot_id = lot.id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON stio .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE stio .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND stio .create_dept_id IN (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = stio.goods_id) " + " AND stio .retail_flag = TRUE "
				+ " AND stio .status = 1 " + " and (stio.yb_complete_flag is not true)"
				+ " and stio.come_from like 'GpcsBack' "
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " ORDER BY doc .busi_date ASC";
		log.info("查询配退数据gpcsbackSql: " + gpcsbackSql);
		pst = conn.prepareStatement(gpcsbackSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询配退数据获取结果集gpcsbackQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String gpcsback = insertOracleStio(orclConn, orclPst, query, customId, loginUrl, 4);
		log.info("添加配退数据完成");
		log.info("修改配退标记--start");
		updateOracleStio(conn, pst, gpcsback, schemalName);
		log.info("修改配退标记--end");

		// -------------报溢
		log.info("查询报溢数据--start");
		String stofSql = "SELECT doc.busi_date , stio.come_from, stio.source_id, stio.create_dept_id, stio.ID, stio.inout_flag, "
				+ " retail .retail_name , retail.retail_code, supply.supply_name, "
				+ " doc.document_code, goods.goods_code, retail.ybshopcode,goods.goods_code as erp_goods_code,"
				+ " ybgoods.yb_goods_code,goods.goods_name,goods.rx_flag,goods.goods_spec,goods.goods_type,goods.goods_unit, "
				+ " lot.prod_date,lot.invalid_date,lot.lot_no, " + " dtl.goods_qty,dtl.unit_price,dtl.amount_money "

		+ " FROM " + schemalName + ".war_stio_doc stio " + " left join " + schemalName
				+ ".war_stof_dtl dtl on stio.source_id = dtl.id " + " left join " + schemalName
				+ ".war_stof_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on stio.goods_id = goods.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on dtl.goods_id = ybgoods.goods_id " + " left join " + schemalName
				+ ".pub_lot lot on dtl.lot_id = lot.id " + " left join " + schemalName
				+ ".pub_supply supply on goods.def_supply_id = supply.id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON stio .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE stio .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND stio .create_dept_id IN (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = stio .goods_id) " + " AND stio .retail_flag = TRUE "
				+ " AND stio .status = 1 " + " and (stio.yb_complete_flag is not true)"
				+ " and stio.come_from like 'StOf' "
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " ORDER BY doc .busi_date ASC";
		log.info("查询报溢数据stofSql: " + stofSql);
		pst = conn.prepareStatement(stofSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询报溢数据获取结果集stofQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String stof = insertOracleStio(orclConn, orclPst, query, customId, loginUrl, 7);
		log.info("添加报溢数据完成");
		log.info("修改报溢标记--start");
		updateOracleStio(conn, pst, stof, schemalName);
		log.info("修改报溢标记--end");

		// ------------报损
		log.info("查询报损数据--start");
		String stlsSql = "SELECT doc.busi_date , stio.come_from, stio.source_id, stio.create_dept_id, stio.ID, stio.inout_flag, "
				+ " retail .retail_name , retail.retail_code, supply.supply_name, "
				+ " doc.document_code, goods.goods_code, retail.ybshopcode,"
				+ " ybgoods.yb_goods_code,goods.goods_name,goods.rx_flag,goods.goods_spec,goods.goods_type,goods.goods_unit, "
				+ " lot.prod_date,lot.invalid_date,lot.lot_no, goods.goods_code as erp_goods_code,"
				+ " dtl.goods_qty,dtl.sk_amount_money,dtl.sk_unit_price "

		+ " FROM " + schemalName + ".war_stio_doc stio " + " left join " + schemalName
				+ ".war_stls_dtl dtl on stio.source_id = dtl.id " + " left join " + schemalName
				+ ".war_stls_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on stio.goods_id = goods.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on dtl.goods_id = ybgoods.goods_id " + " left join " + schemalName
				+ ".pub_lot lot on dtl.lot_id = lot.id " + " left join " + schemalName
				+ ".pub_supply supply on goods.def_supply_id = supply.id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON stio .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE stio .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND stio .create_dept_id IN (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = stio .goods_id) " + " AND stio .retail_flag = TRUE "
				+ " AND stio .status = 1 " + " and stio.come_from like 'StLs' "
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " and (stio.yb_complete_flag is not true)" + " ORDER BY doc .busi_date ASC";
		log.info("查询报损数据stlsSql: " + stlsSql);
		pst = conn.prepareStatement(stlsSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询报损数据获取结果集stlsQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String stls = insertOracleStio(orclConn, orclPst, query, customId, loginUrl, 8);
		log.info("添加报损数据完成");
		log.info("修改报损标记--start");
		updateOracleStio(conn, pst, stls, schemalName);
		log.info("修改报损标记--end");
		log.info("当前用户信息 loginUrl:" + loginUrl + " schemalName: " + schemalName + "================end");
		System.gc();
	}

	private static void SetDefaultFalg(Connection conn, PreparedStatement pst, String schemalName) throws Exception {
		String stioSql = "";
		log.info("出入库设置false--start");
		stioSql = "update " + schemalName + ".war_stio_doc set yb_complete_flag = false where 1=1 and source_entity = 'InitStqtyLst'";
		pst = conn.prepareStatement(stioSql);
		log.info("stioSql :" + stioSql);
		pst.executeUpdate();
		log.info("出入库设置false--end");

	}

	private static void updateOracleStio(Connection conn, PreparedStatement pst, String str, String schemalName)
			throws Exception {
		String[] split = str.split(",");
		int count = 0;
		if (split.length > 0 && !str.equals("")) {
			String updateSql = "";
			updateSql = "update " + schemalName + ".war_stio_doc set yb_complete_flag = true where id = ?";
			log.info("出入库updateSql :" + updateSql);
			pst = conn.prepareStatement(updateSql);
			log.info("执行出入库标记更改--start");
			for (int i = 0; i < split.length; i++) {
				String string = split[i];
				log.info("----------" + string);
				Integer stioId = Integer.valueOf(string);
				pst.setInt(1, stioId);
				pst.addBatch();
				count++;
				if (count % 500 == 0) {
					pst.executeBatch();
					pst.clearBatch();
				}
			}
			pst.executeBatch();
		}
		log.info("执行出入库标记更改--end");
	}

	private static void updateOracleIni(Connection conn, PreparedStatement pst, String insertOracleIni,
			String schemalName) throws Exception {
		String[] split = insertOracleIni.split(",");
		int count = 0;
		if (split.length > 0 && !insertOracleIni.equals("")) {
			log.info("split : " + split);
			String updateSql = "";
			updateSql = "update " + schemalName + ".war_stio_doc set yb_complete_flag = true where id = ?";
			log.info("期初updateSql :" + updateSql);
			pst = conn.prepareStatement(updateSql);
			log.info("执行期初库存标记更改--start");
			for (int i = 0; i < split.length; i++) {
				String string = split[i];
				log.info("----------" + string);
				Integer iniId = Integer.valueOf(string);
				pst.setInt(1, iniId);
				pst.addBatch();
				count++;
				if (count % 500 == 0) {
					pst.executeBatch();
					pst.clearBatch();
				}
			}
			pst.executeBatch();
		}
		log.info("执行期初库存标记更改--end");
	}

	private static String insertOracleIni(Connection oracleconn, PreparedStatement pst, ResultSet rs) throws Exception {
		String str = "";
		int count = 0;
		if(softName == null || softName.equals("")){
			softName = "英克康健";
		}
		String insertSql = "insert into lyg_yb_init_stqty(id,retail_code,retail_name,goods_code,goods_name, goods_spec, goods_type,goods_unit, goods_rx_flag, goods_qty,"
				+ "upload_date, software_name, create_date, erp_goods_code) values(lyg_yb_init_stqty_seq.nextval,?,?,?,?,?,?,?,?,?,sysdate,'"+softName+"',sysdate,?) ";
		log.info("oracle添加语句 sql ：" + insertSql);
		pst = oracleconn.prepareStatement(insertSql);
		while (rs.next()) {
			count++;
			pst.setObject(1, rs.getString("ybshopcode"));
			pst.setObject(2, rs.getString("retail_name"));
			pst.setObject(3, rs.getString("yb_goods_code"));
			pst.setObject(4, rs.getString("goods_name"));
			pst.setObject(5, rs.getString("goods_spec"));
			pst.setObject(6, rs.getString("goods_type"));
			pst.setObject(7, rs.getString("goods_unit"));
			pst.setObject(8, rs.getString("rx_flag"));
			pst.setObject(9, rs.getBigDecimal("goods_qty"));
			pst.setObject(10, rs.getString("erp_goods_code"));
			
			str += rs.getString("id").toString();
			str += ",";
			pst.addBatch();
			if (count % 500 == 0) {
				log.info("累计500条执行executeBatch");
				pst.executeBatch();
				pst.clearBatch();
			}
		}
		log.info("执行期初添加executeBatch");
		pst.executeBatch();
		pst.executeBatch();
		if (str.length() > 0) {
			str = str.substring(0, str.length() - 1);
		}
		log.info("修改出入库标记字符串：" + str);
		return str;
	}

	private static String insertOracleStio(Connection oracleconn, PreparedStatement pst, ResultSet rs, Integer customId,
			String customUrl, int billType) throws Exception, ClassNotFoundException {
		String str = "";
		int count = 0;
		if(softName == null || softName.equals("")){
			softName = "英克康健";
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
		if (billType == 1 || billType == 2) {
			String insertSql = "insert into lyg_yb_war_stio_tmp(id,busi_date,document_code,supply_name,retail_code,retail_name,goods_code,goods_name,"
					+ "goods_spec,goods_type,goods_unit,goods_rx_flag,goods_qty,lot_no,prod_date,invalid_date,unit_price,money,software_name,"
					+ "bill_type,yb_cash_money,yb_id_card,yb_pay_flag,yb_pay_money,yb_user_code,yb_user_name,yb_resa_sex,yb_invoice_no,yb_trade_no,"
					+ "yb_cycle_no,medical_card_number,yb_own_pay,yb_own_expense,yb_tc_money,yb_gwy_money,yb_sb_money,yb_db_money,yb_person_type,"
					+ "yb_area,customerid,customerurl,busi_dtl_id,busi_doc_id,individual_account_pay,resa_price,pack_qty,pack_name,erp_goods_code,"
					+ "receive_money) values (lyg_yb_war_stio_tmp_seq.nextval,?,?,?,?,?,?,?,"
					+ "?,?,?,?,?,?,?,?,?,?,'"+softName+"',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?,?,?,?,?)";
			log.info("oracle添加语句 sql ：" + insertSql);
			pst = oracleconn.prepareStatement(insertSql);
			while (rs.next()) {
				count++;
				Date prodDate = null;
				try {
					prodDate = rs.getDate("prod_date");
					String year = sdf.format(prodDate);
					if (prodDate != null && (Integer.valueOf(year) < 1000 || Integer.valueOf(year) > 2099)) {
						prodDate = new Date();
					}
				} catch (Exception e) {
					prodDate = new Date();
				}

				Date invalDate = null;
				try {
					invalDate = rs.getDate("invalid_date");
					String year = sdf.format(invalDate);
					if (invalDate != null && (Integer.valueOf(year) < 1000 || Integer.valueOf(year) > 2099)) {
						invalDate = new SimpleDateFormat("yyyy-MM-dd").parse("2099-01-01");
					}
				} catch (Exception e) {
					invalDate = new SimpleDateFormat("yyyy-MM-dd").parse("2099-01-01");
				}

				pst.setObject(1, rs.getTimestamp("busi_date"));
				pst.setObject(2, rs.getString("document_code"));
				pst.setObject(3, rs.getString("supply_name"));
				pst.setObject(4, rs.getString("ybshopcode"));
				pst.setObject(5, rs.getString("retail_name"));
				pst.setObject(6, rs.getString("yb_goods_code"));
				pst.setObject(7, rs.getString("goods_name"));
				pst.setObject(8, rs.getString("goods_spec"));
				pst.setObject(9, rs.getString("goods_type"));
				pst.setObject(10, rs.getString("goods_unit"));
				pst.setObject(11, rs.getString("rx_flag"));
				pst.setObject(12, getAbsBigDecimal(rs.getBigDecimal("goods_qty")));
				pst.setObject(13, rs.getString("lot_no"));
				pst.setObject(14, new java.sql.Date(prodDate.getTime()));
				pst.setObject(15, new java.sql.Date(invalDate.getTime()));
				pst.setObject(16, getAbsBigDecimal(rs.getBigDecimal("unit_price")));
				pst.setObject(17, getAbsBigDecimal(rs.getBigDecimal("amount_money")));
				pst.setObject(18, billType);
				pst.setObject(19, getAbsBigDecimal(rs.getBigDecimal("yb_cash_money")));
				pst.setObject(20, rs.getString("yb_id_card"));
				pst.setObject(21, rs.getBoolean("yb_pay_flag") == false ? 0 : 1);
				pst.setObject(22, getAbsBigDecimal(rs.getBigDecimal("yb_pay_money")));
				pst.setObject(23, rs.getString("user_code"));
				pst.setObject(24, rs.getString("yb_user_name"));
				pst.setObject(25, rs.getInt("yb_resa_sex"));
				pst.setObject(26, rs.getString("yb_invoice_no"));
				pst.setObject(27, rs.getString("yb_trade_no"));
				pst.setObject(28, rs.getString("yb_cycle_no"));

				pst.setObject(29, rs.getString("medical_insurance_no"));
				pst.setObject(30, getAbsBigDecimal(rs.getBigDecimal("yb_own_pay")));
				pst.setObject(31, getAbsBigDecimal(rs.getBigDecimal("yb_own_expense")));
				pst.setObject(32, getAbsBigDecimal(rs.getBigDecimal("yb_tc_money")));
				pst.setObject(33, getAbsBigDecimal(rs.getBigDecimal("yb_gwy_money")));
				pst.setObject(34, getAbsString(rs.getString("yb_sb_money")));
				pst.setObject(35, getAbsString(rs.getString("yb_db_money")));
				pst.setObject(36, rs.getString("yb_person_type"));
				pst.setObject(37, rs.getString("yb_area"));

				pst.setObject(38, customId);
				pst.setObject(39, customUrl);
				pst.setObject(40, rs.getInt("id"));
				pst.setObject(41, getAbsBigDecimal(rs.getBigDecimal("yb_pay_money")));
				pst.setObject(42, getAbsBigDecimal(rs.getBigDecimal("resa_price")));
				pst.setObject(43, getAbsBigDecimal(rs.getBigDecimal("pack_qty")));
				pst.setObject(44, rs.getString("pack_name"));
				pst.setObject(45, rs.getString("erp_goods_code"));
				BigDecimal receiveMoney = new BigDecimal("0");
				if(rs.getBigDecimal("goods_qty")!=null&&rs.getBigDecimal("unit_price")!=null ){
					receiveMoney = getAbsBigDecimal(rs.getBigDecimal("goods_qty")).multiply(getAbsBigDecimal(rs.getBigDecimal("unit_price")));
				}
				pst.setObject(46, receiveMoney);

				str += rs.getString("id").toString();
				str += ",";

				pst.addBatch();
				if (count % 500 == 0) {
					log.info("累计500条执行executeBatch");
					pst.executeBatch();
					pst.clearBatch();
				}
			}

		} else if (billType == 8) {
			String insertSql = "insert into lyg_yb_war_stio_tmp(id,busi_date,document_code,supply_name,retail_code,retail_name,goods_code,goods_name,"
					+ " goods_spec,goods_type,goods_unit,goods_rx_flag,goods_qty,lot_no,prod_date,invalid_date,money,software_name,bill_type,"
					+ " customerid, customerurl,busi_dtl_id,busi_doc_id,unit_price,erp_goods_code ) "
					+ " values(lyg_yb_war_stio_tmp_seq.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, '"+softName+"', ?, ?, ?, 0, ?, ?, ? )";
			log.info("oracle添加语句 sql ：" + insertSql);
			pst = oracleconn.prepareStatement(insertSql);
			while (rs.next()) {
				count++;
				Date prodDate = null;
				try {
					prodDate = rs.getDate("prod_date");
					String year = sdf.format(prodDate);
					if (prodDate != null && (Integer.valueOf(year) < 1000 || Integer.valueOf(year) > 2099)) {
						prodDate = new Date();
					}
				} catch (Exception e) {
					prodDate = new Date();
				}

				Date invalDate = null;
				try {
					invalDate = rs.getDate("invalid_date");
					String year = sdf.format(invalDate);
					if (invalDate != null && (Integer.valueOf(year) < 1000 || Integer.valueOf(year) > 2099)) {
						invalDate = new SimpleDateFormat("yyyy-MM-dd").parse("2099-01-01");
					}
				} catch (Exception e) {
					invalDate = new SimpleDateFormat("yyyy-MM-dd").parse("2099-01-01");
				}

				pst.setObject(1, rs.getTimestamp("busi_date"));
				pst.setObject(2, rs.getString("document_code"));
				pst.setObject(3, rs.getString("supply_name"));
				pst.setObject(4, rs.getString("ybshopcode"));
				pst.setObject(5, rs.getString("retail_name"));
				pst.setObject(6, rs.getString("yb_goods_code"));
				pst.setObject(7, rs.getString("goods_name"));
				pst.setObject(8, rs.getString("goods_spec"));
				pst.setObject(9, rs.getString("goods_type"));
				pst.setObject(10, rs.getString("goods_unit"));
				pst.setObject(11, rs.getString("rx_flag"));
				pst.setObject(12, getAbsBigDecimal(rs.getBigDecimal("goods_qty")));
				pst.setObject(13, rs.getString("lot_no"));
				pst.setObject(14, new java.sql.Date(prodDate.getTime()));
				pst.setObject(15, new java.sql.Date(invalDate.getTime()));
				pst.setObject(16, getAbsBigDecimal(rs.getBigDecimal("sk_amount_money")));
				pst.setObject(17, billType);
				pst.setObject(18, customId);
				pst.setObject(19, customUrl);
				pst.setObject(20, rs.getInt("id"));
				pst.setObject(21, getAbsBigDecimal(rs.getBigDecimal("sk_unit_price")));
				pst.setObject(22, rs.getString("erp_goods_code"));
				
				str += rs.getString("id").toString();
				str += ",";

				pst.addBatch();
				if (count % 500 == 0) {
					log.info("累计500条执行executeBatch");
					pst.executeBatch();
					pst.clearBatch();
				}
			}
		} else {
			String insertSql = "insert into lyg_yb_war_stio_tmp(id,busi_date,document_code,supply_name,retail_code,retail_name,goods_code,goods_name,"
					+ " goods_spec,goods_type,goods_unit,goods_rx_flag,goods_qty,lot_no,prod_date,invalid_date,unit_price,money,software_name,bill_type,"
					+ " customerid, customerurl,busi_dtl_id,busi_doc_id,erp_goods_code ) "
					+ " values(lyg_yb_war_stio_tmp_seq.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '"+softName+"', ?, ?, ?, 0, ?, ?)";
			log.info("oracle添加语句 sql ：" + insertSql);
			pst = oracleconn.prepareStatement(insertSql);
			while (rs.next()) {
				count++;
				Date prodDate = null;
				try {
					prodDate = rs.getDate("prod_date");
					String year = sdf.format(prodDate);
					if (prodDate != null && (Integer.valueOf(year) < 1000 || Integer.valueOf(year) > 2099)) {
						prodDate = new Date();
					}
				} catch (Exception e) {
					prodDate = new Date();
				}

				Date invalDate = null;
				try {
					invalDate = rs.getDate("invalid_date");
					String year = sdf.format(invalDate);
					if (invalDate != null && (Integer.valueOf(year) < 1000 || Integer.valueOf(year) > 2099)) {
						invalDate = new SimpleDateFormat("yyyy-MM-dd").parse("2099-01-01");
					}
				} catch (Exception e) {
					invalDate = new SimpleDateFormat("yyyy-MM-dd").parse("2099-01-01");
				}

				pst.setObject(1, rs.getTimestamp("busi_date"));
				pst.setObject(2, rs.getString("document_code"));
				pst.setObject(3, rs.getString("supply_name"));
				pst.setObject(4, rs.getString("ybshopcode"));
				pst.setObject(5, rs.getString("retail_name"));
				pst.setObject(6, rs.getString("yb_goods_code"));
				pst.setObject(7, rs.getString("goods_name"));
				pst.setObject(8, rs.getString("goods_spec"));
				pst.setObject(9, rs.getString("goods_type"));
				pst.setObject(10, rs.getString("goods_unit"));
				pst.setObject(11, rs.getString("rx_flag"));
				pst.setObject(12, getAbsBigDecimal(rs.getBigDecimal("goods_qty")));
				pst.setObject(13, rs.getString("lot_no"));
				pst.setObject(14, new java.sql.Date(prodDate.getTime()));
				pst.setObject(15, new java.sql.Date(invalDate.getTime()));
				pst.setObject(16, getAbsBigDecimal(rs.getBigDecimal("unit_price")));
				pst.setObject(17, getAbsBigDecimal(rs.getBigDecimal("amount_money")));
				pst.setObject(18, billType);
				pst.setObject(19, customId);
				pst.setObject(20, customUrl);
				pst.setObject(21, rs.getInt("id"));
				pst.setObject(22, rs.getString("erp_goods_code"));

				str += rs.getString("id").toString();
				str += ",";

				pst.addBatch();
				if (count % 500 == 0) {
					log.info("累计500条执行executeBatch");
					pst.executeBatch();
					pst.clearBatch();
				}
			}
		}

		log.info("执行最后一次添加executeBatch");
		pst.executeBatch();
		if (str.length() > 0) {
			str = str.substring(0, str.length() - 1);
		}
		log.info("修改出入库标记字符串：" + str);
		return str;
	}

	public static List<Schemal> getSchemalList() {
		List<Schemal> list = new ArrayList<Schemal>();
		try {
			String filePath = "res/bb.txt";
			String encoding = "UTF-8";
			File file = new File(filePath);
			if (file.isFile() && file.exists()) { // 判断文件是否存在
				InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
				BufferedReader bufferedReader = new BufferedReader(read);
				String lineTxt = null;
				while ((lineTxt = bufferedReader.readLine()) != null) {
					log.info("读取文本信息lineTxt: " + lineTxt);
					Schemal schemal = new Schemal();
					String[] split = lineTxt.split(",");
					schemal.setCustomId(Integer.valueOf(split[0]));
					schemal.setSchemalName(split[1]);
					schemal.setSchemalDbName(split[2]);
					schemal.setLoginUrl(split[3]);
					list.add(schemal);
				}
				log.info("list : " + list.size());
				read.close();
			} else {
				log.error("找不到指定的文件");
			}
		} catch (Exception e) {
			log.error("读取文件内容出错");
			e.printStackTrace();
		}
		return list;
	}

	private static void closeResuSet(ResultSet set) {
		if (set != null) {
			try {
				set.close();
			} catch (SQLException e) {
				log.info("数据集关闭异常：" + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	private static void closeConn(Connection con) {
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				log.info("数据连接关闭异常：" + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	private static void closeStatement(Statement stat) {
		if (stat != null) {
			try {
				stat.close();
			} catch (SQLException e) {
				log.info("执行关闭异常：" + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	private static void rollbackCon(Connection con) {
		if (con != null) {
			try {
				con.rollback();
			} catch (SQLException e) {
				log.info("连接回滚异常：" + e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	private static BigDecimal getAbsBigDecimal(BigDecimal value) {
		if (value != null && value.compareTo(new BigDecimal("0")) < 0) {
			value = value.abs();
			return value;
		}else if (value != null && value.compareTo(new BigDecimal("0")) >= 0){
			return value;
		}else{
			return new BigDecimal("0");
		}
	}
	
	private static String getAbsString(String value) {
		if (value != null && !value.equals("") && new BigDecimal(value).compareTo(new BigDecimal("0")) < 0) {
			BigDecimal newValue = new BigDecimal(value).abs();
			return newValue + "";
		}else if (value != null && !value.equals("") && new BigDecimal(value).compareTo(new BigDecimal("0")) >= 0) {
			return value;
		}else{
			return "0";
		}
	}

	private static int getIsThird(Connection pgconn, PreparedStatement pst, String schemalName) throws SQLException {
		log.info("查询" + schemalName + " ---" +" 是否第三方软件商--start");
		String idSql = "SELECT third_party,software_name from " + schemalName + ".sys_company as third_party limit 1";
		log.info("查询sys_company 是否第三方软件商: " + idSql);
		pst = pgconn.prepareStatement(idSql);// 准备执行语句
		ResultSet query = pst.executeQuery();
		int thirdParty = 0;
		while (query.next()) {
			thirdParty = query.getInt("third_party");
			softName = query.getString("software_name");
		}
		log.info("查询sys_company 是否第三方软件商: " + thirdParty + softName);
		return thirdParty;
	}
}
