import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.session import SparkSession
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.functions import col, concat
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import split
from pyspark.sql.functions import year, month, dayofmonth
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv,
                          ['source_db',
                           'output_bucket_name','output_db'])
                           
source_db = args['source_db']  
output_bucket = args['output_bucket_name']
output_db = args['output_db']


def create_Bounceback_table(source_db):
    edw_bounceback_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='bounceback').toDF()
    edw_assets_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='assets').toDF()
    edw_campaign_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='campaign').toDF()

    joined_df = edw_bounceback_df.join(edw_assets_df, edw_bounceback_df.assetwid == edw_assets_df.rowwid, 'left').join(edw_campaign_df, edw_campaign_df.rowwid == edw_bounceback_df.campaignwid, 'left').select(edw_bounceback_df["integrationid"].alias("uniqueid").cast('string'), F.split(edw_campaign_df["datasourcecode"], "_")[2].alias("businessunitcode").cast('string'), edw_bounceback_df["activityid"].cast('string'), edw_campaign_df["datasourcecode"].alias("activitytype").cast('string'), edw_bounceback_df["activitydate"].cast('string'),edw_bounceback_df["emailaddress"].cast('string'),edw_assets_df["assetname"].cast('string'), edw_assets_df["assettype"].cast('string'),edw_assets_df["integrationid"].alias("assetid").cast('string'),edw_campaign_df["integrationid"].alias("campaignid").cast('string'), edw_bounceback_df["integrationid"].alias("externalid").cast('string'))
    bounceback_df = joined_df.withColumn("businessunitcode",  concat(F.lit("OBU_"), joined_df.businessunitcode)).withColumn("activitytype",F.lit("Bounceback").cast('string')).withColumn("exportcorrelationid", F.lit(None).cast('string')).withColumn("exportdate", current_timestamp().cast('string'))
    return bounceback_df
    
def create_Campaign_table(source_db):
    edw_campaign_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='campaign').toDF()
    edw_commchannel_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='communicationchannel').toDF()
    edw_eventedition_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='eventedition').toDF()
    edw_event_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='event').toDF()
    
    joined_df = edw_campaign_df.join(edw_commchannel_df, edw_campaign_df.commchannelwid == edw_commchannel_df.rowwid, 'left').join(edw_eventedition_df, edw_eventedition_df.rowwid == edw_campaign_df.eventedwid, 'left').join(edw_event_df, edw_event_df.rowwid == edw_campaign_df.eventwid, 'left').select(edw_campaign_df["integrationid"].alias("id").cast('string'), edw_campaign_df["mcaname"].alias("name").cast('string'), F.split(edw_campaign_df["datasourcecode"], "_")[2].alias("businessunitcode").cast('string'), edw_campaign_df["sfdccampaignid"].alias("sfdcid").cast('string'), edw_campaign_df["purpose"].alias("campaigntype").cast('string'), edw_campaign_df["status"].cast('string'), edw_commchannel_df["integrationid"].alias("communicationchannelid").cast('string'), edw_campaign_df["startdatetime"].alias("startat").cast('string'), edw_campaign_df["enddatetime"].alias("endat").cast('string'), edw_eventedition_df["integrationid"].alias("eventeditioncode").cast('string'), edw_event_df["eventnumericcode"].cast('string'), edw_event_df["eventalphacode"].cast('string'), edw_campaign_df["purposeofcampaign"].cast('string'), edw_campaign_df["customerlifecycle"].cast('string'), edw_campaign_df["customertype"].cast('string'), edw_campaign_df["eventeditioncode"].alias("eventeditionyear").cast('string'), edw_campaign_df["createdondt"].alias("createdat").cast('string'), edw_campaign_df["createdby"].cast('string'),  edw_campaign_df["wupdatedt"].alias("updatedat").cast('string'), edw_campaign_df["loadupdatedby"].alias("updatedby").cast('string'))
    campaign_df = joined_df.withColumn("businessunitcode",  concat(F.lit("OBU_"), joined_df.businessunitcode)).withColumn("exportcorrelationid", F.lit(None).cast('string')).withColumn("exportdate", current_timestamp().cast('string'))
    return campaign_df
    

def create_EmailClickthrough_table(source_db):
    edw_emailclickthrough_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='emailclickthrough').toDF()
    edw_campaign_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='campaign').toDF()
    edw_visitor_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='visitor').toDF()
    edw_assets_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='assets').toDF()
    
    joined_df = edw_emailclickthrough_df.join(edw_campaign_df, edw_emailclickthrough_df.campaignwid == edw_campaign_df.rowwid, 'left').join(edw_visitor_df, edw_visitor_df.rowwid == edw_emailclickthrough_df.visitorwid, 'left').join(edw_assets_df, edw_assets_df.rowwid == edw_emailclickthrough_df.assetwid, 'left').select(edw_emailclickthrough_df["integrationid"].alias("uniqueid").cast('string'), F.split(edw_campaign_df["datasourcecode"], "_")[2].alias("businessunitcode").cast('string'), edw_emailclickthrough_df["activityid"].cast('string'), edw_campaign_df["datasourcecode"].alias("activitytype").cast('string'), edw_emailclickthrough_df["activitydate"].cast('string'), edw_emailclickthrough_df["emailaddress"].cast('string'), edw_emailclickthrough_df["contactid"].cast('string'), edw_emailclickthrough_df["ipaddress"].cast('string'), edw_visitor_df["integrationid"].alias("visitorid").cast('string'), edw_emailclickthrough_df["emailrecipientid"].cast('string'), edw_assets_df["assettype"].cast('string'), edw_assets_df["assetname"].cast('string'), edw_assets_df["integrationid"].alias("assetid").cast('string'), edw_emailclickthrough_df["subjectline"].cast('string'), edw_emailclickthrough_df["emailweblink"].cast('string'), edw_emailclickthrough_df["emailclickedthrulink"].cast('string'), edw_visitor_df["visitorexternalid"].cast('string'), edw_campaign_df["integrationid"].alias("campaignid").cast('string'), edw_emailclickthrough_df["externalid"].cast('string'), edw_emailclickthrough_df["deploymentid"].cast('string'), edw_emailclickthrough_df["emailsendtype"].cast('string'))
    emailclickthrough_df = joined_df.withColumn("activitytype", F.lit("EmailClickThrough").cast('string')).withColumn("businessunitcode",  concat(F.lit("OBU_"), joined_df.businessunitcode)).withColumn("exportcorrelationid", F.lit(None).cast('string')).withColumn("exportdate", current_timestamp().cast('string'))
    return emailclickthrough_df
    
    
def create_EmailOpen_table(source_db):
    edw_emailopen_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='emailopen').toDF()
    edw_campaign_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='campaign').toDF()
    edw_visitor_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='visitor').toDF()
    edw_assets_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='assets').toDF()
    
    joined_df = edw_emailopen_df.join(edw_campaign_df, edw_emailopen_df.campaignwid == edw_campaign_df.rowwid, 'left').join(edw_visitor_df, edw_visitor_df.rowwid == edw_emailopen_df.visitorwid, 'left').join(edw_assets_df, edw_assets_df.rowwid == edw_emailopen_df.assetwid, 'left').select(edw_emailopen_df["integrationid"].alias("uniqueid").cast('string'), F.split(edw_campaign_df["datasourcecode"], "_")[2].alias("businessunitcode").cast('string'), edw_emailopen_df["activityid"].cast('string'), edw_campaign_df["datasourcecode"].alias("activitytype").cast('string'), edw_emailopen_df["activitydate"].cast('string'), edw_emailopen_df["emailaddress"].cast('string'), edw_emailopen_df["contactid"].cast('string'), edw_emailopen_df["ipaddress"].cast('string'), edw_visitor_df["integrationid"].alias("visitorid").cast('string'), edw_emailopen_df["emailrecipientid"].cast('string'), edw_assets_df["assettype"].cast('string'), edw_assets_df["assetname"].cast('string'), edw_assets_df["integrationid"].alias("assetid").cast('string'), edw_emailopen_df["subjectline"].cast('string'), edw_emailopen_df["emailweblink"].cast('string'), edw_visitor_df["visitorexternalid"].cast('string'), edw_campaign_df["integrationid"].alias("campaignid").cast('string'), edw_emailopen_df["integrationid"].alias("externalid").cast('string'), edw_emailopen_df["deploymentid"].cast('string'), edw_emailopen_df["emailsendtype"].cast('string'))
    emailopen_df = joined_df.withColumn("activitytype", F.lit("EmailOpen").cast('string')).withColumn("businessunitcode",  concat(F.lit("OBU_"), joined_df.businessunitcode)).withColumn("exportcorrelationid", F.lit(None).cast('string')).withColumn("exportdate", current_timestamp().cast('string'))
    return emailopen_df
    

def create_PageView_table(source_db):
    print("################Inside PageView#################")
    edw_pageview_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='pageview').toDF()
    edw_campaign_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='campaign').toDF()
    edw_visitor_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='visitor').toDF()
    
    joined_df = edw_pageview_df.join(edw_campaign_df, edw_pageview_df.campaignwid == edw_campaign_df.rowwid, 'left').join(edw_visitor_df, edw_visitor_df.rowwid == edw_pageview_df.visitorwid, 'left').select(edw_pageview_df["integrationid"].alias("uniqueid").cast('string'), F.split(edw_campaign_df["datasourcecode"], "_")[2].alias("businessunitcode").cast('string'), edw_pageview_df["activityid"].cast('string'), edw_campaign_df["datasourcecode"].alias("activitytype").cast('string'), edw_pageview_df["activitydate"].cast('string'), edw_pageview_df["contactid"].cast('string'), edw_campaign_df["integrationid"].alias("campaignid").cast('string'), edw_visitor_df["integrationid"].alias("visitorid").cast('string'), edw_visitor_df["visitorexternalid"].cast('string'), edw_pageview_df["webvisitid"].cast('string'), edw_pageview_df["url"].cast('string'), edw_pageview_df["referrerurl"].cast('string'), edw_pageview_df["ipaddress"].cast('string'), edw_pageview_df["iswebtrackingoptedin"].cast('string'), edw_pageview_df["integrationid"].alias("externalid").cast('string'))
    pageview_df = joined_df.withColumn("activitytype", F.lit("PageView").cast('string')).withColumn("businessunitcode",  concat(F.lit("OBU_"), joined_df.businessunitcode)).withColumn("exportcorrelationid", F.lit(None).cast('string')).withColumn("exportdate", current_timestamp().cast('string'))
    return pageview_df
    
def create_WebVisit_table(source_db):
    print("################Inside WebVisit#################")
    edw_webvisit_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='webvisit').toDF()
    edw_campaign_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='campaign').toDF()
    edw_visitor_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='visitor').toDF()
    
    joined_df = edw_webvisit_df.join(edw_campaign_df, edw_webvisit_df.campaignwid == edw_campaign_df.rowwid, 'left').join(edw_visitor_df, edw_visitor_df.rowwid == edw_webvisit_df.visitorwid, 'left').select(edw_webvisit_df["integrationid"].alias("uniqueid").cast('string'), F.split(edw_campaign_df["datasourcecode"], "_")[2].alias("businessunitcode").cast('string'), edw_webvisit_df["activityid"].cast('string'), edw_campaign_df["datasourcecode"].alias("activitytype").cast('string'), edw_webvisit_df["activitydate"].cast('string'), edw_webvisit_df["contactid"].cast('string'), edw_visitor_df["integrationid"].alias("visitorid").cast('string'), edw_visitor_df["visitorexternalid"].cast('string'), edw_webvisit_df["referrerurl"].cast('string'), edw_webvisit_df["ipaddress"].cast('string'), edw_webvisit_df["numberofpages"].cast('string'), edw_webvisit_df["firstpageviewurl"].cast('string'), edw_webvisit_df["duration"].cast('string'), edw_webvisit_df["integrationid"].alias("externalid").cast('string'))
    webvisit_df = joined_df.withColumn("activitytype", F.lit("WebVisit").cast('string')).withColumn("businessunitcode",  concat(F.lit("OBU_"), joined_df.businessunitcode)).withColumn("exportcorrelationid", F.lit(None).cast('string')).withColumn("exportdate", current_timestamp().cast('string'))
    return webvisit_df
    
def create_EmailSend_table(source_db):
    print("################Inside EmailSend#################")
    edw_emailsend_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='emailsend').toDF()
    edw_campaign_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='campaign').toDF()
    edw_assets_df = glueContext.create_dynamic_frame.from_catalog(database=str(source_db), table_name='assets').toDF()
    
    joined_df = edw_emailsend_df.join(edw_campaign_df, edw_emailsend_df.campaign_wid == edw_campaign_df.rowwid, 'left').join(edw_assets_df, edw_assets_df.rowwid == edw_emailsend_df.asset_wid, 'left').select(edw_emailsend_df["integration_id"].alias("uniqueid").cast('string'), F.split(edw_campaign_df["datasourcecode"], "_")[2].alias("businessunitcode").cast('string'), edw_emailsend_df["activity_id"].alias("activityid").cast('string'), edw_campaign_df["datasourcecode"].alias("activitytype").cast('string'), edw_emailsend_df["activity_date"].alias("activitydate").cast('string'), edw_emailsend_df["email_address"].alias("emailaddress").cast('string'), edw_emailsend_df["contact_id"].alias("contactid").cast('string'), edw_emailsend_df["email_recipient_id"].alias("emailrecipientid").cast('string'), edw_assets_df["assettype"].cast('string'), edw_assets_df["integrationid"].alias("assetid").cast('string'), edw_assets_df["assetname"].cast('string'), edw_emailsend_df["subject_line"].alias("subjectline").cast('string'), edw_emailsend_df["email_web_link"].alias("emailweblink").cast('string'), edw_campaign_df["integrationid"].alias("campaignid").cast('string'), edw_emailsend_df["integration_id"].alias("externalid").cast('string'), edw_emailsend_df["deployment_id"].alias("deploymentid").cast('string'), edw_emailsend_df["email_send_type"].alias("emailsendtype").cast('string'))
    emailsend_df = joined_df.withColumn("activitytype", F.lit("EmailSend").cast('string')).withColumn("businessunitcode",  concat(F.lit("OBU_"), joined_df.businessunitcode)).withColumn("exportcorrelationid", F.lit(None).cast('string')).withColumn("exportdate", current_timestamp().cast('string'))
    return emailsend_df
    
    

def saveeloquatable(final_df, output_bucket, table_name):
    output_df = final_df.withColumn("BU_name", final_df.businessunitcode).withColumn("Year", year(final_df.exportdate)).withColumn("Month", month(final_df.exportdate)).withColumn("Day", dayofmonth(final_df.exportdate))
    print("************WebVisit Table*********")
    output_df.write.saveAsTable(table_name, path = "s3://" + str(output_bucket)+"/Eloqua/"+ table_name + "/", mode="overwrite", compression="snappy", partitionBy=["BU_name","Year","Month","Day"])
    

try:
    sc = SparkContext()
    spark = SparkSession(sc)
    glueContext = GlueContext(sc)
    sparksession = glueContext.spark_session
    job = Job(glueContext)
    sparksession.catalog.setCurrentDatabase(output_db)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    Table_names = ["Bounceback","Campaign","EmailClickthrough","EmailOpen","EmailSend","PageView","WebVisit"]
    for x in Table_names:
        fname = f'create_{x}_table'
        function = locals()[fname]
        final_df = function(source_db)
        saveeloquatable(final_df, output_bucket, table_name = x)
    job.commit()

except Exception as error:
    raise error
