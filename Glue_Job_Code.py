import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
import concurrent.futures
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Movies-from-S3
MoviesfromS3_node1724465635451 = glueContext.create_dynamic_frame.from_catalog(database="movies-db", table_name="movies_rawinput_data", transformation_ctx="MoviesfromS3_node1724465635451")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1724465676101_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        ColumnValues "imdb_rating" between 8.5 and 10.3,
        IsComplete "imdb_rating"
    ]
"""

EvaluateDataQuality_node1724465676101 = EvaluateDataQuality().process_rows(frame=MoviesfromS3_node1724465635451, ruleset=EvaluateDataQuality_node1724465676101_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1724465676101", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1724470184776 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1724465676101, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1724470184776")

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1724470189938 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1724465676101, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1724470189938")

# Script generated for node Conditional Router
ConditionalRouter_node1724470455238 = threadedRoute(glueContext,
  source_DyF = rowLevelOutcomes_node1724470189938,
  group_filters = [GroupFilter(name = "failed_records", filters = lambda row: (bool(re.match("Failed", row["DataQualityEvaluationResult"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("Failed", row["DataQualityEvaluationResult"])))))])

# Script generated for node failed_records
failed_records_node1724470456352 = SelectFromCollection.apply(dfc=ConditionalRouter_node1724470455238, key="failed_records", transformation_ctx="failed_records_node1724470456352")

# Script generated for node default_group
default_group_node1724470456173 = SelectFromCollection.apply(dfc=ConditionalRouter_node1724470455238, key="default_group", transformation_ctx="default_group_node1724470456173")

# Script generated for node Change Schema
ChangeSchema_node1724470732190 = ApplyMapping.apply(frame=default_group_node1724470456173, mappings=[("poster_link", "string", "poster_link", "string"), ("series_title", "string", "series_title", "string"), ("released_year", "string", "released_year", "string"), ("certificate", "string", "certificate", "string"), ("runtime", "string", "runtime", "string"), ("genre", "string", "genre", "string"), ("imdb_rating", "double", "imdb_rating", "double"), ("overview", "string", "overview", "string"), ("meta_score", "long", "meta_score", "long"), ("director", "string", "director", "string"), ("star1", "string", "star1", "string"), ("star2", "string", "star2", "string"), ("star3", "string", "star3", "string"), ("star4", "string", "star4", "string"), ("no_of_votes", "long", "no_of_votes", "long"), ("gross", "string", "gross", "string")], transformation_ctx="ChangeSchema_node1724470732190")

# Script generated for node Rule Outcome ETL
RuleOutcomeETL_node1724470309347 = glueContext.write_dynamic_frame.from_options(frame=ruleOutcomes_node1724470184776, connection_type="s3", format="json", connection_options={"path": "s3://movies-data-yb/rule_outcome_from_etl/", "partitionKeys": []}, transformation_ctx="RuleOutcomeETL_node1724470309347")

# Script generated for node bad_records S3
bad_recordsS3_node1724470629697 = glueContext.write_dynamic_frame.from_options(frame=failed_records_node1724470456352, connection_type="s3", format="json", connection_options={"path": "s3://movies-data-yb/bad_records/", "partitionKeys": []}, transformation_ctx="bad_recordsS3_node1724470629697")

# Script generated for node Redshift Target Table
RedshiftTargetTable_node1724484655516 = glueContext.write_dynamic_frame.from_catalog(frame=ChangeSchema_node1724470732190, database="movies-db", table_name="dev_movies_imdb_movies_rating", redshift_tmp_dir="s3://redshift-temp-yb",additional_options={"aws_iam_role": "arn:aws:iam::010526265053:role/service-role/AmazonRedshift-CommandsAccessRole-20240806T210859"}, transformation_ctx="RedshiftTargetTable_node1724484655516")

job.commit()