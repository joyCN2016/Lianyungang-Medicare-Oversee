package inca.saas.mess.lyg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Category;

public class ThirtyHelper {

	static Category log = Category.getInstance(ThirtyHelper.class);

	protected static void process(Connection conn, PreparedStatement pst, ResultSet query, List<Schemal> dblist, Schemal schemal) throws Exception {
		Integer customId = schemal.getCustomId();
		String loginUrl = schemal.getLoginUrl();
		String schemalName = schemal.getSchemalName();
		log.info("当前用户信息 loginUrl:" + loginUrl + " schemalName: " + schemalName);
		
		log.info("把出入库细单数据删除--start");
		Deletetable("war_stio_dtl", conn, pst, schemalName);
		log.info("把出入库细单数据删除--end");
		
		log.info("把出入库总单数据删除--start");
		Deletetable("war_stio_doc", conn, pst, schemalName);
		log.info("把出入库总单数据删除--end");
		
		log.info("把出入库数据删除--start");
		Deletetable("lybyb_retail", conn, pst, schemalName);
		log.info("把出入库数据删除--end");
		
		log.info("处理医保门店导lybybretail表--start");
		createYbRetail(conn, pst, schemalName);
		log.info("处理医保门店导lybybretail表--end");
		
		Integer retailId = selectIsOneYBretail(conn, pst, schemalName);
		
//		// --------------------------期初
		log.info("查询期初库存数据--start");
		String iniSql = "SELECT dtl.id, dtl.dept_id, dtl.goods_id, SUM (dtl.goods_qty) AS goods_qty, dtl.busi_date "
				+ " FROM " + schemalName 
				+ ".war_stqty_his_lst dtl left join " + schemalName 
				+ ".pub_goods goods on dtl.goods_id = goods.id  "
				+ "LEFT JOIN " + schemalName 
				+ ".yb_goods ybgoods ON dtl.goods_id = ybgoods.goods_id "
				+ "LEFT JOIN " + schemalName 
				+ ".pub_dept dept ON dtl.dept_id = dept. ID "
				+ "LEFT JOIN " + schemalName 
				+ ".pub_retail retail ON retail. ID = dept. ID "
				+ "WHERE dtl.dept_id = retail. ID "
				+ "AND dtl.goods_id IN ( SELECT goods_id FROM " + schemalName 
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' "
				+ "AND ( yb_goods_code IS NOT NULL AND yb_goods_code != '' ) ) "
				+ "AND dtl.dept_id IN ( SELECT A .retail_id FROM " + schemalName 
				+ ".lybyb_retail A "
				+ "LEFT JOIN " + schemalName 
				+ ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ "A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '' ) "
				+ " and (dtl.yb_complete_flag is not true)" + "AND  NOT EXISTS ( SELECT 1 FROM " + schemalName 
				+ ".sys_hide_goods WHERE goods_id = dtl.goods_id ) "
//				+ "and dtl.busi_date = (select min(busi_date) from " + schemalName 
//				+ ".war_stqty_his_lst) "
				+ " and dtl.memo = '期初' "
				+ "GROUP BY dtl.id,dtl.goods_id, dtl.dept_id, dtl.busi_date";
		log.info("查询期初库存数据iniSql: " + iniSql);
		pst = conn.prepareStatement(iniSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询期初库存数据获取结果集: " + query);
		
		insertPgStio(conn, pst, query, schemalName, 0);
		log.info("添加期初库存数据完成");
		
		// --------------配送
		log.info("查询配送数据--start");
		String gpcsdeliverySql = " SELECT doc .busi_date ,  dtl .id, doc.retail_id as dept_id, dtl.goods_id,dtl.goods_qty "

		+ " FROM " + schemalName
				+ ".dis_gpcs_delivery_dtl dtl left join " + schemalName
				+ ".dis_gpcs_delivery_doc doc on dtl.doc_id = doc.id " + "	left join " + schemalName
				+ ".pub_goods goods on dtl.goods_id = goods.id "  + " left join " + schemalName
				+ ".yb_goods ybgoods on goods.id = ybgoods.goods_id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON doc .retail_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE dtl .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND doc .retail_id IN (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = dtl.goods_id) " 
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " ORDER BY doc .busi_date ASC";
		log.info("查询配退数据gpcsdeliverySql: " + gpcsdeliverySql);
		pst = conn.prepareStatement(gpcsdeliverySql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询配送数据获取结果集gpcsdeliverySqlQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String insertOracleStio = insertPgStio(conn, pst, query, schemalName, 3);
		log.info("添加配送数据完成");

		// ---------------采退
		log.info("查询采退数据--start");
		String purchasebackSql = " SELECT dtl.id, doc.dept_id, doc.busi_date, dtl.goods_id, dtl.goods_qty "

		+ " FROM " + schemalName
				+ ".pur_purchase_dtl dtl " + " left join " + schemalName
				+ ".pur_purchase_doc doc on dtl.doc_id = doc.id " + "	left join " + schemalName
				+ ".pub_goods goods on dtl.goods_id = goods.id " + " left join " + schemalName
				+ ".pub_supply supply on goods.def_supply_id = supply.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on goods.id = ybgoods.goods_id " + " left join " + schemalName
				+ ".pub_lot lot on dtl.lot_id = lot.id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON doc .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE dtl .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND doc .create_dept_id IN (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = dtl.goods_id) " + " and supply.self_flag is not true "
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '')  and doc.pur_type = 2"
				+ " ORDER BY doc .busi_date ASC";
		//判断是否为单体店 直接取唯一的医保门店
		if(!retailId.equals(0)){
			purchasebackSql = " SELECT doc .busi_date ,  dtl .id, "+retailId+" as dept_id, dtl.goods_id,dtl.goods_qty "

		+ " FROM " + schemalName
				+ ".pur_purchase_dtl dtl left join " + schemalName
				+ ".pur_purchase_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on dtl.goods_id = goods.id " + " left join " + schemalName
				+ ".pub_supply supply on goods.def_supply_id = supply.id "
				+ " where supply.self_flag is not true  and doc.pur_type = 2"
				+ " ORDER BY doc .busi_date ASC";
		}
		log.info("查询采退数据purchasebackSql: " + purchasebackSql);
		pst = conn.prepareStatement(purchasebackSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询采退数据获取结果集purchasebackQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String purchaseback = insertPgStio(conn, pst, query, schemalName, 10);
		log.info("添加采退数据完成");

		// -----------零退
		log.info("查询零退数据--start");
		String resaBackSql = " SELECT doc .busi_date ,  dtl .id, doc.retail_id as dept_id, dtl.goods_id,dtl.goods_qty "

		+ " FROM " + schemalName
				+ ".rsa_resa_dtl dtl left join " + schemalName
				+ ".rsa_resa_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on dtl.goods_id = goods.id "  + " left join " + schemalName
				+ ".yb_goods ybgoods on dtl.goods_id = ybgoods.goods_id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON doc .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE dtl .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND doc .retail_id IN (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = dtl .goods_id) and doc.resa_type = 2" 
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " ORDER BY doc .busi_date ASC";
		log.info("查询零退数据resaBackSql: " + resaBackSql);
		pst = conn.prepareStatement(resaBackSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询零退数据获取结果集resaBackQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String resaBack = insertPgStio(conn, pst, query, schemalName, 2);
		log.info("添加零退数据完成");

		// -----------零售
		log.info("查询零售数据--start");
		String resaSql = " SELECT doc .busi_date ,  dtl .id, doc.retail_id as dept_id, dtl.goods_id,dtl.goods_qty "

		+ " FROM " + schemalName
				+ ".rsa_resa_dtl dtl  left join " + schemalName
				+ ".rsa_resa_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on dtl.goods_id = goods.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on dtl.goods_id = ybgoods.goods_id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON doc .retail_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE dtl .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND doc.retail_id IN (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = dtl .goods_id) "
				+ " and doc.resa_type = 1 "
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " ORDER BY doc .busi_date ASC ";
		log.info("查询零售数据resaSql: " + resaSql);
		pst = conn.prepareStatement(resaSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询零售数据获取结果集resaQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String resa = insertPgStio(conn, pst, query, schemalName, 1);
		log.info("添加零售数据完成");

		// --------------采购
		log.info("查询采购数据--start");
		String purchaseSql = " SELECT doc .busi_date ,  dtl .id, doc.create_dept_id as dept_id, dtl.goods_id,dtl.goods_qty "

		+ " FROM " + schemalName
				+ ".pur_purchase_dtl dtl left join " + schemalName
				+ ".pur_purchase_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on dtl.goods_id = goods.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on dtl.goods_id = ybgoods.goods_id " + " left join " + schemalName
				+ ".pub_supply supply on doc.supply_id = supply.id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON doc .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE dtl .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND doc .create_dept_id IN " + " (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = dtl.goods_id) "
				+ " and supply.self_flag is not true  and doc.pur_type = 1"
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " ORDER BY doc .busi_date ASC";
		//判断是否为单体店 直接取唯一的医保门店
		if(!retailId.equals(0)){
			purchaseSql = " SELECT doc .busi_date ,  dtl .id, "+retailId+" as dept_id, dtl.goods_id,dtl.goods_qty "

		+ " FROM " + schemalName
				+ ".pur_purchase_dtl dtl left join " + schemalName
				+ ".pur_purchase_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on dtl.goods_id = goods.id " + " left join " + schemalName
				+ ".pub_supply supply on goods.def_supply_id = supply.id "
				+ " where supply.self_flag is not true  and doc.pur_type = 1"
				+ " ORDER BY doc .busi_date ASC";
		}
		log.info("查询采购数据purchaseSql: " + purchaseSql);
		pst = conn.prepareStatement(purchaseSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询采购数据获取结果集purchaseQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String purchase = insertPgStio(conn, pst, query, schemalName, 9);
		log.info("添加采购数据完成");

		// --------------配退
		log.info("查询配退数据--start");
		String gpcsbackSql = " SELECT doc .busi_date ,  dtl .id, doc.create_dept_id as dept_id, dtl.goods_id,dtl.goods_qty "

		+ " FROM " + schemalName
				+ ".dis_gpcs_back_dtl dtl left join " + schemalName
				+ ".dis_gpcs_back_doc doc on dtl.doc_id = doc.id " + "	left join " + schemalName
				+ ".pub_goods goods on dtl.goods_id = goods.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on goods.id = ybgoods.goods_id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON doc .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE dtl .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND doc .create_dept_id IN (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = dtl.goods_id) " 
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " ORDER BY doc .busi_date ASC";
		log.info("查询配退数据gpcsbackSql: " + gpcsbackSql);
		pst = conn.prepareStatement(gpcsbackSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询配退数据获取结果集gpcsbackQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String gpcsback = insertPgStio(conn, pst, query, schemalName, 4);
		log.info("添加配退数据完成");

		// -------------报溢
		log.info("查询报溢数据--start");
		String stofSql = " SELECT doc .busi_date ,  dtl .id, doc.create_dept_id as dept_id, dtl.goods_id,dtl.goods_qty "

		+ " FROM " + schemalName
				+ ".war_stof_dtl dtl left join " + schemalName
				+ ".war_stof_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on dtl.goods_id = goods.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on dtl.goods_id = ybgoods.goods_id " + " left join " + schemalName
				+ ".pub_supply supply on goods.def_supply_id = supply.id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON doc .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE dtl .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND doc .create_dept_id IN (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = dtl .goods_id) " 
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " ORDER BY doc .busi_date ASC";
		//判断是否为单体店 直接取唯一的医保门店
		if(!retailId.equals(0)){
			stofSql = " SELECT doc .busi_date ,  dtl .id, "+retailId+" as dept_id, dtl.goods_id,dtl.goods_qty "

		+ " FROM " + schemalName
				+ ".war_stof_dtl dtl left join " + schemalName
				+ ".war_stof_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on dtl.goods_id = goods.id " 
				+ " ORDER BY doc .busi_date ASC";
		}
		log.info("查询报溢数据stofSql: " + stofSql);
		pst = conn.prepareStatement(stofSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询报溢数据获取结果集stofQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String stof = insertPgStio(conn, pst, query, schemalName, 7);
		log.info("添加报溢数据完成");

		// ------------报损
		log.info("查询报损数据--start");
		String stlsSql = " SELECT doc .busi_date ,  dtl .id, doc.create_dept_id as dept_id, dtl.goods_id,dtl.goods_qty "

		+ " FROM " + schemalName
				+ ".war_stls_dtl dtl left join " + schemalName
				+ ".war_stls_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on dtl.goods_id = goods.id " + " left join " + schemalName
				+ ".yb_goods ybgoods on dtl.goods_id = ybgoods.goods_id " + " left join " + schemalName
				+ ".pub_supply supply on goods.def_supply_id = supply.id " + " LEFT JOIN " + schemalName
				+ ".lybyb_retail ybretail ON doc .create_dept_id = ybretail.retail_id " + " LEFT JOIN "
				+ schemalName + ".pub_retail retail ON ybretail.retail_id = retail . ID "

		+ " WHERE dtl .goods_id IN " + " (SELECT goods_id FROM " + schemalName
				+ ".yb_goods WHERE yb_type_code LIKE 'YB_LYG_JG%' AND (yb_goods_code IS NOT NULL AND yb_goods_code != '')) "
				+ " AND doc .create_dept_id IN (SELECT A .retail_id FROM " + schemalName
				+ ".lybyb_retail A LEFT JOIN " + schemalName + ".pub_retail b ON A .retail_id = b. ID WHERE "
				+ " A .yb_type_code LIKE 'YB_LYG_JG%' AND b.ybshopcode IS NOT NULL AND b.ybshopcode != '') "
				+ " AND NOT EXISTS (SELECT 1 FROM " + schemalName
				+ ".sys_hide_goods WHERE goods_id = dtl .goods_id) "
				+ " and (ybgoods.yb_goods_code is not null and ybgoods.yb_goods_code != '') "
				+ " ORDER BY doc .busi_date ASC";
		//判断是否为单体店 直接取唯一的医保门店
		if(!retailId.equals(0)){
			stlsSql = " SELECT doc .busi_date ,  dtl .id, "+retailId+" as dept_id, dtl.goods_id,dtl.goods_qty "

		+ " FROM " + schemalName
				+ ".war_stls_dtl dtl left join " + schemalName
				+ ".war_stls_doc doc on dtl.doc_id = doc.id " + " left join " + schemalName
				+ ".pub_goods goods on dtl.goods_id = goods.id " 
				+ " ORDER BY doc .busi_date ASC";
		}
		log.info("查询报损数据stlsSql: " + stlsSql);
		pst = conn.prepareStatement(stlsSql);// 准备执行语句
		query = pst.executeQuery();
		log.info("查询报损数据获取结果集stlsQuery: " + query + " customId :" + customId + " loginUrl :" + loginUrl);
		String stls = insertPgStio(conn, pst, query, schemalName, 8);
		log.info("添加报损数据完成");
		log.info("当前用户信息 loginUrl:" + loginUrl + " schemalName: " + schemalName + "================end");
		System.gc();
	}

	private static void Deletetable(String tableName, Connection conn, PreparedStatement pst, String schemalName) throws Exception {
		String stioSql = "";

		log.info("第三方 " + tableName +"  删除--start");
		stioSql = "delete from " + schemalName + "."+tableName+" where 1=1";
		pst = conn.prepareStatement(stioSql);
		log.info("stioSql :" + stioSql);
		pst.execute();
		log.info("第三方 " + tableName +"  删除--end");

	}


	//零售1零退2 配送3配退4 其它入5其它出6 报溢7报损8采购9采退10
	private static String insertPgStio(Connection pgconn, PreparedStatement pst, ResultSet rs, String schemalName, int billType) throws Exception, ClassNotFoundException {
		String str = "";
		int count = 0;
		String comeFrom = "";
		String sourceEntiey = "";
		int docId = 0;
		int dtlId = 0;
		if (billType == 0){
			comeFrom = "Stini";
			sourceEntiey = "InitStqtyLst";
		}else if(billType == 1 || billType == 2){
			comeFrom = "ResaDoc";
		}else if(billType == 3){
			comeFrom = "StTransfer";
		}else if( billType == 9){
			comeFrom = "SuPur";
		}else if(billType == 10){
			comeFrom = "SuBack";
		}else if(billType == 8){
			comeFrom = "StLs";
		}else if(billType == 7){
			comeFrom = "StOf";
		}else if(billType == 4){
			comeFrom = "GpcsBack";
		}
		String insertDocSql = "insert into "+schemalName+".war_stio_doc(id,_version,create_dept_id,goods_id,come_from,source_entity,source_id, status, busi_date, retail_id,retail_flag) "
				+ "values(?,0,?,?,?,?,?,1,?,?,true) ";
		log.info("pg stioDoc添加语句 sql ：" + insertDocSql);
		pst = pgconn.prepareStatement(insertDocSql);
		
		String insertDtlSql = "insert into "+schemalName+".war_stio_dtl(id,doc_id,_version,create_dept_id,goods_qty) "
				+ "values(?,?,0,?,?) ";
		log.info("pg insertDtl添加语句 sql ：" + insertDtlSql);
		PreparedStatement dtlPst = pgconn.prepareStatement(insertDtlSql);
		
		HashMap<Integer,Integer> map = new HashMap<>();
		while (rs.next()) {
			count++;
			log.info("第" + count + "条数据！");
			docId = getTableId("war_stio_doc", pgconn, pst, schemalName);
			pst.setObject(1, docId);
			pst.setObject(2, rs.getInt("dept_id"));
			pst.setObject(3, rs.getInt("goods_id"));
			pst.setObject(4, comeFrom);
			pst.setObject(5, sourceEntiey);
			pst.setObject(6, rs.getInt("id"));
			pst.setObject(7, rs.getDate("busi_date"));
			pst.setObject(8, rs.getInt("dept_id"));
			pst.addBatch();
			if (count % 500 == 0) {
				log.info("累计500条执行executeBatch");
				pst.executeBatch();
				pst.clearBatch();
			}
			
			map.put(rs.getInt("id"), docId);
			
			dtlId = getTableId("war_stio_dtl", pgconn, pst, schemalName);

			dtlPst.setObject(1, dtlId);
			dtlPst.setObject(2, docId);
			dtlPst.setObject(3, rs.getInt("dept_id"));
			dtlPst.setObject(4, rs.getBigDecimal("goods_qty").abs());

			dtlPst.addBatch();
			if (count % 500 == 0) {
				log.info("累计500条执行executeBatch");
				dtlPst.executeBatch();
				dtlPst.clearBatch();
			}
		}
		
		log.info("执行最后一次doc执行executeBatch");
		pst.executeBatch();
		pst.clearBatch();
		
		log.info("执行最后一次dtl执行executeBatch");
		dtlPst.executeBatch();
		dtlPst.clearBatch();
		log.info("执行生成出入库单的逻辑 添加单据数：" + count);
		
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

	private static int getTableId(String tableName, Connection pgconn, PreparedStatement pst, String schemalName) throws SQLException {
		log.info("查询" +tableName +" 序列--start");
		String idSql = "SELECT * from nextval('" + schemalName + "."+tableName+"_id_seq') as id";
		log.info("查询" +tableName +" 序列: " + idSql);
		pst = pgconn.prepareStatement(idSql);// 准备执行语句
		ResultSet query = pst.executeQuery();
		log.info("查询" +tableName +" 序列获取结果集: " + query);
		int id = 0;
		while (query.next()) {
			id = query.getInt("id");
		}
		return id;
	}
	

	private static void createYbRetail(Connection conn, PreparedStatement pst, String schemalName) throws SQLException {
		String idSql = "SELECT * from "+schemalName+".pub_retail where ybshopcode  != '' and ybshopcode is not null";
		pst = conn.prepareStatement(idSql);// 准备执行语句
		ResultSet rs = pst.executeQuery();
		log.info("查询期初库存数据获取结果集: " + rs);
		String insertretailSql = "insert into "+schemalName+".lybyb_retail(id,retail_id,_version,yb_type_code) "
				+ "values(?,?,0,'YB_LYG_JG') ";
		log.info("pg insertretailSql添加语句 sql ：" + insertretailSql);
		pst = conn.prepareStatement(insertretailSql);
		int id =0;
		int count = 0;
		while (rs.next()) {
			count++;
			id = getTableId("lybyb_retail", conn, pst, schemalName);
			pst.setObject(1, id);
			pst.setObject(2, rs.getInt("id"));

			pst.addBatch();
			if (count % 500 == 0) {
				log.info("累计500条执行executeBatch");
				pst.executeBatch();
				pst.clearBatch();
			}
		}
		log.info("执行生成出入库单的逻辑 添加executeBatch");
		pst.executeBatch();
		log.info("执行最后一次生成出入库单 executeBatch");
		pst.clearBatch();
	}
	
	
	private static Integer selectIsOneYBretail(Connection conn, PreparedStatement pst, String schemalName) throws SQLException {
		String idSql = "SELECT * from "+schemalName+".pub_retail where ybshopcode  != '' and ybshopcode is not null";
		pst = conn.prepareStatement(idSql);// 准备执行语句
		ResultSet rs = pst.executeQuery();
		log.info("查询期初库存数据获取结果集: " + rs);
		int count = 0;
		Integer retailId = null;
		while (rs.next()) {
			count++;
			retailId = rs.getInt("id");
		}
//		if(count != 0){
//			return 0;
//		}else{
			return retailId;
//		}

	}
}
