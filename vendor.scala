package Heena

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.sql.types;
import org.apache.spark.sql.SparkSession

//PROJECT NO 2
//SPARKSQL PROJECT USING SQLQUERIES AND REGISTERTEMPTABLE
//KMART CRITICAL PROCESS
//DEVELOPED BY HEENASALIM SHAIKH
//DAYS SPENT : -8   HOURS :-32 HOURS

object vendor {
  
  def main(args:Array[String])
  {
    
   //Reading the files in dataframe forms
   //The spark.read.csv function is dataframe 
   //when we pass case class/type to this data frame like sparkread.csv.as[Person]/csv.as[String]
   //it becomes dataset  which can be handle exacty as method 1 project1 ie select and filter
     
     val sparkconf = new SparkConf().setAppName("Test").setMaster("local[*]")
     sparkconf.set("spark.sql.crossJoin.enabled", "true")
     val sparksession = SparkSession.builder().config(sparkconf).master("local").
     appName("kmart_vendor_pakage_location").getOrCreate();
     import sparksession.implicits._
    
     var gold__item_aprk_current_data
     = sparksession.read.format("csv")
     .option("header","true")
     .option("delimiter","|")
     .option("inferSchema","true")
     .load("C:\\Users\\jabin\\Desktop\\project_files\\gold_item_aprk_current_data.txt")
         
    var work__store_level_vend_pack_loc_final_data =
      
      sparksession.read.format("csv")
     .option("header", "true")
     .option("delimiter", "|")
     .option("inferSchema", "true")
     .load("C:\\Users\\jabin\\Desktop\\project_files\\work__store_level_vend_pack_loc_final_data.txt");
     
      work__store_level_vend_pack_loc_final_data.registerTempTable("work__store_level_vend_pack_loc_final_data_table");
     
    var work__idrp_vp_dc_start = sparksession.sqlContext.sql(
  "SELECT distinct * from (select shc_item_id ,'K' as source_owner_cd,ksn_id,item_purchase_status_cd, vendor_package_id,vendor_package_purchase_status_cd,vendor_package_purchase_status_dt,flow_type_cd as vendor_package_flow_type_cd,vendor_carton_qty,vendor_stock_nbr,ksn_package_id,ksn_purchase_status_cd,import_ind ,sears_division_nbr,sears_item_nbr,sears_sku_nbr,scan_based_trading_ind,cross_merchandising_cd,retail_carton_vendor_package_id,vendor_package_owner_cd,can_carry_model_id,'' AS days_to_check_begin_day_qty,'' AS days_to_check_end_day_qty ,dotcom_orderable_cd ,retail_carton_internal_package_qty,scan_based_trading_ind as scan_based_trading_ind1,shc_item_type_cd,idrp_order_method_cd,source_package_qty as store_source_package_qty,order_duns_nbr FROM work__store_level_vend_pack_loc_final_data_table WHERE flow_type_cd = 'JIT'  OR servicing_dc_nbr > '0') ")
     
     // sparksession.sqlContext.sql("select distinct *  from work__store_level_vend_pack_loc_final_data_table")
//.collect.foreach(println);
    

    var load_smith__idrp_eligible_loc_data =
      
    sparksession.read.format("csv")
    .option("delimiter", "|")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("C:\\Users\\jabin\\Desktop\\project_files\\SMITH_IDRP_ELIGIBLE_LOC_LOCATION.txt")
  
    load_smith__idrp_eligible_loc_data.registerTempTable(" smith__idrp_eligible_loc_data_table");
    
     
    var  smith__idrp_eligible_loc_data = sparksession.sqlContext.sql("select loc,srs_loc,loc_lvl_cd, fmt_typ_cd,loc_fmt_typ_cd,loc_owner_cd from smith__idrp_eligible_loc_data_table" )
      
      
     var smith__idrp_vend_pack_dc_combined_data =
     sparksession.read.format("csv")
     .option("delimiter", "|")
     .option("header", "true")
     .option("inferSchema", "true")
     .load("C:\\Users\\jabin\\Desktop\\project_files\\smith__idrp_vend_pack_dc_combined_data.txt")
     
     smith__idrp_vend_pack_dc_combined_data.registerTempTable("smith__idrp_vend_pack_dc_combined_data_table")
     sparksession.sqlContext.sql("select vendor_package_id,location_nbr,inbound_carton_per_layer_qty,inbound_layer_per_pallet_qty,inbound_order_uom_cd,ship_aprk_id,ksn_pack_purchase_status_cd,ksn_package_id,dc_stock_ind as stock_ind,substitution_eligible_ind,outbound_package_qty,ship_duns_nbr,effective_ts,expiration_ts,vendor_managed_inventory_cd,dc_handling_cd from smith__idrp_vend_pack_dc_combined_data_table ")
    
     var smith__idrp_inbound_vendor_package_dc_driver_data =
     sparksession.read.format("csv") 
    .option("delimiter", "|")
    .option("header", "true")
    .option("inferSchema","true")
    .load("C:\\Users\\jabin\\Desktop\\project_files\\smith__idrp_inbound_vendor_package_dc_driver_data.txt")

    smith__idrp_inbound_vendor_package_dc_driver_data.registerTempTable("smith__idrp_inbound_vendor_package_dc_driver_data_table")
    
    sparksession.sqlContext.sql("select item_id as inbnd_item_id, vendor_package_id as inbnd_vend_pack_id,dc_location_nbr as inbnd_dc_loc_nbr,replenishment_planning_ind as replenishment_planning_ind  from smith__idrp_inbound_vendor_package_dc_driver_data_table")
    
     var  work__idrp_kmart_vendor_package_location_store_level_data =
     sparksession.read.format("csv")
    .option("delimiter", "|")
    .option("header", "true")
    .option("inferSchema","true")
    .load("C:\\Users\\jabin\\Desktop\\project_files\\work__idrp_kmart_vendor_package_location_store_level_data.txt")
   
    work__idrp_kmart_vendor_package_location_store_level_data.registerTempTable("work__idrp_kmart_vendor_package_location_store_level_data_table")
  
    sparksession.sqlContext.sql("select * from work__idrp_kmart_vendor_package_location_store_level_data_table where active_ind = 'Y'")
    
   // .collect().foreach(println)
   var smith__idrp_dc_location_current_data =
     sparksession.read.format("csv")
     .option("delimiter","|")
     .option("header","true")
     .option("inferSchema", "true")
     .load("C:\\Users\\jabin\\Desktop\\project_files\\smith__idrp_dc_location_current_data.txt")

     smith__idrp_dc_location_current_data.registerTempTable("smith__idrp_dc_location_current_data_table")
     
     var smith__idrp_ie_batchdate_data =
     sparksession.read.format("csv")
     .option("header", "true")
     .option("delimiter", "|")
     .option("inferSchema" ,"true")
     .load("C:\\Users\\jabin\\Desktop\\project_files\\smith__idrp_ie_batchdate_data .txt")
      
     smith__idrp_ie_batchdate_data.registerTempTable("smith__idrp_ie_batchdate_data_table");
    
    import java.util.Properties;
    
    sparksession.sqlContext.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') AS StartTime").show()
     var smith_join_smith = sparksession.
     sqlContext.sql(
//"SELECT * FROM (    
     "SELECT shc_item_id,source_owner_cd,ksn_id,item_purchase_status_cd,vendor_package_id,vendor_package_purchase_status_cd,vendor_package_purchase_status_dt, \n " +
     "vendor_package_flow_type_cd,vendor_carton_qty,vendor_stock_nbr,ksn_package_id,ksn_purchase_status_cd,import_ind1,sears_division_nbr,sears_item_nbr,\n" +
     "sears_sku_nbr,scan_based_trading_ind as scan_based_trading_ind,cross_merchandising_cd,retail_carton_vendor_package_id,vendor_package_owner_cd,can_carry_model_id,days_to_check_begin_day_qty,  \n " + 
     "days_to_check_end_day_qty ,dotcom_orderable_cd,retail_carton_internal_package_qty,shc_item_type_cd,ksn_dc_package_purchase_status_cd,ship_duns_nbr,stock_ind1,substitution_eligible_ind,"+
     "outbound_package_qty,ship_aprk_id,inbound_order_uom_cd,carton_per_layer_qty,location_id ,\n"+
     "case when import_ind = '1' and location_id != '8277'  then '8277' else concat(trim(ship_duns_nbr),'_S') end  as source_location_id," +
     "concat(ship_duns_nbr,'_S') as purchase_order_vendor_location_id ,\n" +
     "layer_per_pallet_qty,store_source_package_qty,order_duns_nbr,vendor_managed_inventory_cd,dc_handling_cd ,dc_flowthru_ind,enable_jif_dc_ind, \n"+
     
     "case when vendor_package_flow_type_cd = 'JIT' then 'JIT'else \n " +
          "case when dc_flowthru_ind = 'Y' and enable_jif_dc_ind = 'N' then 'FLT' else \n" +
                "case when dc_flowthru_ind = 'Y' and enable_jif_dc_ind = 'Y' then 'JIF' else \n" +
                    "case when dc_flowthru_ind = 'N' then 'STK' else '' \n" +
                     "end \n" +
                "end \n"   +
          "end \n"   +
     "end as dc_configuration_cd ,allocation_replenishment_cd\n"  +             
     " FROM( \n" +
     
                            "SELECT shc_item_id,source_owner_cd,ksn_id,item_purchase_status_cd,vendor_package_id,vendor_package_purchase_status_cd,vendor_package_purchase_status_dt,vendor_package_flow_type_cd,vendor_carton_qty,vendor_stock_nbr,ksn_package_id,ksn_purchase_status_cd,import_ind as import_ind1,sears_division_nbr,sears_item_nbr,sears_sku_nbr,scan_based_trading_ind,cross_merchandising_cd,retail_carton_vendor_package_id,vendor_package_owner_cd,can_carry_model_id,days_to_check_begin_day_qty,days_to_check_end_day_qty ,retail_carton_internal_package_qty,allocation_replenishment_cd,shc_item_type_cd,idrp_order_method_cd,store_source_package_qty,order_duns_nbr,ship_duns_nbr,location_nbr as location_id, \n" +
                            "ksn_pack_purchase_status_cd as ksn_dc_package_purchase_status_cd,stock_ind as stock_ind1,substitution_eligible_ind,outbound_package_qty, ship_aprk_id,inbound_order_uom_cd,inbound_carton_per_layer_qty as carton_per_layer_qty,inbound_layer_per_pallet_qty as layer_per_pallet_qty,store_source_package_qty,order_duns_nbr,vendor_managed_inventory_cd,dc_handling_cd ,allocation_replenishment_cd, \n" +
                            "case when import_ind = 1 then 'N'  \n" +
                            "else\n" +
                            "case when vendor_managed_inventory_cd is not null or trim(vendor_managed_inventory_cd) != '' \n"  +
                                      "and vendor_managed_inventory_cd = '5'  or vendor_managed_inventory_cd = '6'  \n"  +
                                      "or  vendor_managed_inventory_cd = '7'  or vendor_managed_inventory_cd = '8'  \n"  +
                                      "or  vendor_managed_inventory_cd = '5'  or vendor_managed_inventory_cd = '9'  \n"  +
                                "then 'N' \n" +
                                "else \n " +
                                    "case when stock_ind='N' and IsNull(dc_handling_cd != '') and dc_handling_cd is not null and dc_handling_cd ='CASE' then 'Y' else 'N' end \n" +    
                                "end \n" +
                            "end as dc_flowthru_ind,dotcom_orderable_cd \n" +
                            "FROM  (\n" +
                                          "(SELECT vendor_package_id1,location_nbr,inbound_carton_per_layer_qty,inbound_layer_per_pallet_qty,inbound_order_uom_cd,ship_aprk_id,ksn_pack_purchase_status_cd,stock_ind,substitution_eligible_ind,outbound_package_qty,ship_duns_nbr,vendor_managed_inventory_cd,dc_handling_cd  FROM \n" +
                                                          "(select * from smith__idrp_ie_batchdate_data_table) a  \n" +
                  /*smith__join_smith*/                                  " CROSS JOIN \n" + 
                                                          "(select vendor_package_id as vendor_package_id1,location_nbr,inbound_carton_per_layer_qty,inbound_layer_per_pallet_qty,inbound_order_uom_cd,ship_aprk_id,ksn_pack_purchase_status_cd,ksn_package_id,dc_stock_ind as stock_ind,substitution_eligible_ind,outbound_package_qty,ship_duns_nbr,effective_ts,expiration_ts,vendor_managed_inventory_cd,dc_handling_cd from smith__idrp_vend_pack_dc_combined_data_table) b \n" +
                                           "WHERE trim(a.processing_ts) >= trim(b.effective_ts) and trim(a.processing_ts) <= trim(b.effective_ts) and b.location_nbr!='8277' \n " +
                                           ")                \n" +
                                           "INNER JOIN \n" +
                                           "(SELECT distinct * FROM \n " +
                                                           "(select shc_item_id ,'K' as source_owner_cd,ksn_id,item_purchase_status_cd, vendor_package_id,vendor_package_purchase_status_cd, vendor_package_purchase_status_dt, flow_type_cd as vendor_package_flow_type_cd,vendor_carton_qty,vendor_stock_nbr,ksn_package_id,ksn_purchase_status_cd,\n" + 
                                                           "import_ind,sears_division_nbr,sears_item_nbr,sears_sku_nbr,scan_based_trading_ind,cross_merchandising_cd,retail_carton_vendor_package_id,vendor_package_owner_cd,can_carry_model_id,'' AS days_to_check_begin_day_qty,'' AS days_to_check_end_day_qty ,dotcom_orderable_cd,retail_carton_internal_package_qty, \n" +
                 /* work_idrp_vp_dc_start*/                "allocation_replenishment_cd,shc_item_type_cd,idrp_order_method_cd,source_package_qty as store_source_package_qty,order_duns_nbr FROM work__store_level_vend_pack_loc_final_data_table \n" +
                                                           "where flow_type_cd = 'JIT'  OR servicing_dc_nbr > '0') \n"  + 
                                           ")         \n  "     +
                             ")  \n"      +                                          
            ") A \n"    + 
             "LEFT OUTER JOIN \n" +
             "(SELECT * FROM smith__idrp_dc_location_current_data_table) B on A.location_id = B.dc_location_nbr  "
   // " )  \n" 
              )
            
             .createOrReplaceTempView("work__join_gold_gen_table")
              //.collect.foreach(println);
    

//-----------------------------------------------------------------------------------------------------------------------------------    
    
    val load_work__idrp_dummy_vend_whse_ref =
      sparksession.read.format("csv")
     .option("delimiter","|")
     .option("header","true")
     .option("inferSchema", "true")
     .load("C:\\Users\\jabin\\Desktop\\project_files\\work__idrp_dummy_vend_whse_ref.txt");

    load_work__idrp_dummy_vend_whse_ref.registerTempTable("work__idrp_dummy_vend_whse_ref_table")
    sparksession.sqlContext.sql(
   "select vendor_nbr,warehouse_nbr from work__idrp_dummy_vend_whse_ref_table");
     
   //spliting based on columns 
  //  val work__join_gold_gen_cross_mse_attr_cd_fltr=
      
sparksession.sqlContext.sql(

"SELECT ksn_id, \n" + 
"count(vendor_package_id),\n" + 
"vendor_package_id \n"  +
"FROM " +
"(" +
         "SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') AS load_ts, \n" +
          "ksn_id, \n" + 
          "vendor_package_id," +
          "item_purchase_status_cd ,\n" +
          "location_format_type_cd, \n " +
          "location_level_cd, \n" +
          "location_owner_cd, \n" +
          "source_owner_cd, \n" + 
          "active_ind, \n" + 
          "active_ind_change_dt, \n" +
          "allocation_replenishment_cd, \n" +
          "replenishment_planning_id, \n" +
          "scan_based_trading_ind, \n" + 
          "source_location_id, \n" + 
          "source_location_level_cd, \n" +
          "case when inbound_order_uom_cd is not null  \n" +
          "then \n " +   
                         "case when inbound_order_uom_cd = 'LAYR' \n "  +
                         "then vendor_carton_qty  * carton_per_layer_qty \n" +
                         "else \n" +
                                    "case when inbound_order_uom_cd = 'PALL' \n "	 +
                                    "then vendor_carton_qty * carton_per_layer_qty * layer_per_pallet_qty \n" +
                                    "else vendor_carton_qty \n" +
                                    "end \n " +
                         "end \n" +
          "else vendor_carton_qty  \n"  +
          "end \n " +
          "as source_package_qty , \n" +       
          "vendor_package_purchase_status_cd, \n" + 
          "vendor_package_purchase_status_dt, \n" + 
          "flow_type_cd, \n" +
          "import_ind, \n" +
          "retail_carton_vendor_package_id1, \n" +
          "vendor_package_owner_cd, \n" +
          "vendor_stock_nbr, \n" +
          "shc_item_id, \n" + 
          "item_purchase_status_cd, \n" +
          "can_carry_model_id, \n" +
          "days_to_check_begin_day_qty, \n" +
          "days_to_check_end_day_qty, \n" +
          "reorder_method_cd, \n" +
          "ksn_purchase_status_cd, \n" +
          "cross_merchandising_cd, \n" +
          "dotcom_orderable_cd, \n" +
          "kmart_markdown_ind, \n" +
          "ksn_package_id, \n" +
          "ksn_dc_package_purchase_status_cd, \n" +
          "dc_configuration_cd, \n" +
          "substitution_eligible_ind, \n" +
          "sears_division_nbr \n" +
          "sears_sku_nbr, \n" +
          "sears_location_id, \n" +
          "sears_source_location_id, \n" +
          "rim_status_cd, \n" +
          "stock_type_cd, \n" +
          "non_stock_source_cd, \n" +
          "dos_item_active_ind, \n" +
          "dos_item_reserve_cd, \n" +
          "create_dt,\n" +
          "last_update_dt, \n" +
          "shc_item_type_cd, \n" +
          "format_typ_cd, \n" +
          "outbound_package_qty, \n" +
          "retail_carton_internal_package_qty, \n" +
          "vendor_carton_qty, \n" +
          "enable_jif_dc_ind, " +
          "'1902-01-01' as rim_last_record_creation_dt, \n" +
          "batchid " +
          "FROM( "+ 
                         "SELECT * FROM work__idrp_kmart_vendor_package_location_store_level_data_table p \n"+
                         "LEFT OUTER JOIN   (" +
                                                   "SELECT shc_item_id as shc_item_id1 ,source_owner_cd as source_owner_cd1,ksn_id as ksn_id1,item_purchase_status_cd as item_purchase_status_cd1,vendor_package_id as vendor_package_id1,vendor_package_purchase_status_cd as vendor_package_purchase_status_cd1,vendor_package_purchase_status_dt as vendor_package_purchase_status_dt1, \n " +
                                                   "vendor_package_flow_type_cd ,vendor_carton_qty as vendor_carton_qty1,vendor_stock_nbr as vendor_stock_nbr1,ksn_package_id as ksn_package_id1,ksn_purchase_status_cd as ksn_purchase_status_cd1,import_ind1,sears_division_nbr as sears_division_nbr1,sears_item_nbr as sears_item_nbr1,\n" +
                                                   "sears_sku_nbr as sears_sku_nbr1,scan_based_trading_ind as scan_based_trading_ind1,cross_merchandising_cd as cross_merchandising_cd1,retail_carton_vendor_package_id as retail_carton_vendor_package_id1,vendor_package_owner_cd as vendor_package_owner_cd1 ,can_carry_model_id as can_carry_model_id1,days_to_check_begin_day_qty as days_to_check_begin_day_qty1,  \n " + 
                                                   "days_to_check_end_day_qty as days_to_check_end_day_qty1,dotcom_orderable_cd as dotcom_orderable_cd1,retail_carton_internal_package_qty as retail_carton_internal_package_qty1,shc_item_type_cd as shc_item_type_cd1,ksn_dc_package_purchase_status_cd as ksn_dc_package_purchase_status_cd1,ship_duns_nbr,stock_ind1,location_id as location_id1 ,\n"+
                                                   "substitution_eligible_ind as ubstitution_eligible_ind1,outbound_package_qty as outbound_package_qty1,ship_aprk_id,inbound_order_uom_cd,carton_per_layer_qty,layer_per_pallet_qty,  \n"+
                                                   "source_location_id as source_location_id1, purchase_order_vendor_location_id,\n " +
                                                   "allocation_replenishment_cd as allocation_replenishment_cd1,store_source_package_qty \n" +
                                                   " FROM  (  \n"  +
                                                                         "SELECT shc_item_id,source_owner_cd,ksn_id,item_purchase_status_cd,vendor_package_id,vendor_package_purchase_status_cd,vendor_package_purchase_status_dt ,\n " +
                                                                         "vendor_package_flow_type_cd,vendor_carton_qty,vendor_stock_nbr,ksn_package_id,ksn_purchase_status_cd,import_ind1,sears_division_nbr,sears_item_nbr,\n" +
                                                                         "sears_sku_nbr,scan_based_trading_ind,cross_merchandising_cd,retail_carton_vendor_package_id,vendor_package_owner_cd,can_carry_model_id,days_to_check_begin_day_qty,  \n " + 
                                                                         "days_to_check_end_day_qty ,dotcom_orderable_cd,retail_carton_internal_package_qty,shc_item_type_cd,ksn_dc_package_purchase_status_cd,ship_duns_nbr,stock_ind1,location_id ,\n"+
                                                                         "substitution_eligible_ind ,outbound_package_qty,ship_aprk_id,inbound_order_uom_cd,carton_per_layer_qty,layer_per_pallet_qty,  \n"+
                                                                         "case when IsNull(vendor_nbr) !='' then string(int(trim(warehouse_nbr))) else concat(trim(ship_duns_nbr),'S') end as source_location_id, \n"  +
                                                                         "case when IsNull(vendor_nbr) != ''then ' ' else concat(trim(ship_duns_nbr,'_S'))  end as purchase_order_vendor_location_id,\n " +
                                                                         "enable_jif_dc_ind,dc_configuration_cd, allocation_replenishment_cd,store_source_package_qty,order_duns_nbr,dc_flowthru_ind,vendor_managed_inventory_cd,dc_handling_cd \n" +
                                                                         "FROM ( \n" +
                                                                                        "SELECT * FROM work__idrp_dummy_vend_whse_ref_table A  \n" +
                                                                                       " RIGHT OUTER JOIN \n" +  
                                                                                       "(SELECT * FROM work__join_gold_gen_table where cross_merchandising_cd = 'SK1400') B on int(TRIM(order_duns_nbr)) = int(TRIM(vendor_nbr))  \n "  +
                                                                         ") " +
                                                                    //  ") " +                        =
                                                                         "UNION select * from work__join_gold_gen_table where cross_merchandising_cd != 'SK1400' or cross_merchandising_cd is null \n"      
                                             +   ")   \n"  +
                                      " )  q " +
                     "ON p.vendor_package_id = q.vendor_package_id1 and p.source_location_id = q.location_id1  "  +
          ") "  +  //END OF SELECT   
") \n"     //END OF COUNT 

+ "GROUP BY vendor_package_id,ksn_id"
   )
   sparksession.sqlContext.sql("select * from work__join_gold_gen_table where cross_merchandising_cd != 'SK1400' or cross_merchandising_cd is null");
    
  }
  
  //AIM IS COMPLETED 
  
}
