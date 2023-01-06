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

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1672948634692 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCuratedZone_node1672948634692",
)

# Script generated for node Steptrainer Landing Node
SteptrainerLandingNode_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="steptrainer_landing",
    transformation_ctx="SteptrainerLandingNode_node1",
)

# Script generated for node Renamed keys for Join Customer Node
RenamedkeysforJoinCustomerNode_node1672985934066 = ApplyMapping.apply(
    frame=CustomerCuratedZone_node1672948634692,
    mappings=[
        ("customername", "string", "`(right) customername`", "string"),
        ("email", "string", "`(right) email`", "string"),
        ("phone", "string", "`(right) phone`", "string"),
        ("birthday", "string", "`(right) birthday`", "string"),
        ("serialnumber", "string", "`(right) serialnumber`", "string"),
        ("registrationdate", "long", "`(right) registrationdate`", "long"),
        ("lastupdatedate", "long", "`(right) lastupdatedate`", "long"),
        (
            "sharewithresearchasofdate",
            "long",
            "`(right) sharewithresearchasofdate`",
            "long",
        ),
        (
            "sharewithpublicasofdate",
            "long",
            "`(right) sharewithpublicasofdate`",
            "long",
        ),
    ],
    transformation_ctx="RenamedkeysforJoinCustomerNode_node1672985934066",
)

# Script generated for node Join Customer Node
JoinCustomerNode_node2 = Join.apply(
    frame1=SteptrainerLandingNode_node1,
    frame2=RenamedkeysforJoinCustomerNode_node1672985934066,
    keys1=["serialnumber"],
    keys2=["`(right) serialnumber`"],
    transformation_ctx="JoinCustomerNode_node2",
)

# Script generated for node Drop Fields
DropFields_node1672948935151 = DropFields.apply(
    frame=JoinCustomerNode_node2,
    paths=[],
    transformation_ctx="DropFields_node1672948935151",
)

# Script generated for node Steptrainer Trusted Node
SteptrainerTrustedNode_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1672948935151,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://lbl-udacity/steptrainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="SteptrainerTrustedNode_node3",
)

job.commit()
