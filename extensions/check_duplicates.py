import json
from sqlalchemy import create_engine
from trino.sqlalchemy import URL
import pandas as pd

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
from nifiapi.relationship import Relationship


class TrinoCheckDuplicates(FlowFileTransform):

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            "Checks whether given flowfile content is already contained in database "
            "using SQL query and comparing provided columns"
        )
        dependencies = ['trino', 'sqlalchemy', 'pandas']

    TRINO_HOST = PropertyDescriptor(
        name="Trino Host",
        description="URL to Trino server",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.ENVIRONMENT
    )

    TRINO_PORT = PropertyDescriptor(
        name="Trino Port",
        description="Port of Trino server",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.ENVIRONMENT
    )

    TRINO_CATALOG = PropertyDescriptor(
        name="Trino Catalog",
        description="Catalog of Trino DB",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.ENVIRONMENT
    )

    TRINO_USER = PropertyDescriptor(
        name="Trino User",
        description="User of Trino DB",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.ENVIRONMENT
    )

    TRINO_PASSWORD = PropertyDescriptor(
        name="Trino Password",
        description="Password of Trino DB",
        required=False,
        sensitive=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.ENVIRONMENT
    )

    SQL_QUERY = PropertyDescriptor(
        name="SQL query",
        description="Query executed against database",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    COLUMN_MAPPING = PropertyDescriptor(
        name="Column mapping",
        description="JSON mapping between flowfile fields and database columns",
        required=True,
        default_value="{}",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    properties = [
        TRINO_HOST,
        TRINO_PORT,
        TRINO_CATALOG,
        TRINO_USER,
        TRINO_PASSWORD,
        SQL_QUERY,
        COLUMN_MAPPING
    ]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.properties

    def getRelationships(self):
        return {
            Relationship("success", description="Flowfiles that are not duplicates are routed to this relationship"),
            Relationship("duplicate", description="Flowfiles that are duplicates are routed to this relationship"),
        }

    def transform(self, context, flowfile):

        contents_bytes = flowfile.getContentsAsBytes()
        contents = contents_bytes.decode('utf-8')
        flow_data = json.loads(contents)

        if isinstance(flow_data, dict):
            flow_data = [flow_data]

        sql = context.getProperty(self.SQL_QUERY).evaluateAttributeExpressions(flowfile).getValue()
        column_mapping = json.loads(
            context.getProperty(self.COLUMN_MAPPING)
            .evaluateAttributeExpressions(flowfile)
            .getValue()
        )

        engine = create_engine(
            URL(
                host=context.getProperty(self.TRINO_HOST).evaluateAttributeExpressions(flowfile).getValue(),
                port=context.getProperty(self.TRINO_PORT).evaluateAttributeExpressions(flowfile).getValue(),
                catalog=context.getProperty(self.TRINO_CATALOG).evaluateAttributeExpressions(flowfile).getValue(),
                user=context.getProperty(self.TRINO_USER).evaluateAttributeExpressions(flowfile).getValue(),
                password=context.getProperty(self.TRINO_PASSWORD).evaluateAttributeExpressions(flowfile).getValue()
            )
        )
        with engine.connect() as connection:
            df = pd.read_sql_query(sql, connection)
        db_rows = df.to_dict(orient='records')

        non_duplicates = []
        duplicates = []

        for item in flow_data:
            is_duplicate = False

            for db_row in db_rows:
                if all(
                        str(item.get(flow_col)) == str(db_row.get(db_col))
                        for flow_col, db_col in column_mapping.items()
                ):
                    is_duplicate = True
                    duplicates.append(item)
                    break

            if not is_duplicate:
                non_duplicates.append(item)

        if non_duplicates:
            return FlowFileTransformResult(
                relationship="success",
                contents=json.dumps(non_duplicates)
            )
        
        if duplicates:
            return FlowFileTransformResult(
                relationship="duplicate",
                contents=json.dumps(duplicates)
            )

        return None
