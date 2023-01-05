import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1672948634692 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1672948634692",
)

# Script generated for node Accelerometer Landing Node
AccelerometerLandingNode_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLandingNode_node1",
)

# Script generated for node Join Customer Node
JoinCustomerNode_node2 = Join.apply(
    frame1=AccelerometerLandingNode_node1,
    frame2=CustomerTrustedZone_node1672948634692,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomerNode_node2",
)

# Script generated for node Drop Fields
DropFields_node1672948935151 = DropFields.apply(
    frame=JoinCustomerNode_node2,
    paths=["email", "phone"],
    transformation_ctx="DropFields_node1672948935151",
)

# Script generated for node Accelerometer Trusted Node
AccelerometerTrustedNode_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1672948935151,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://lbl-udacity/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrustedNode_node3",
)

job.commit()
