from dagger.dag_creator.airflow.operators.awsbatch_operator import AWSBatchOperator

class ReverseEtlBatchOperator(AWSBatchOperator):
    custom_operator_name = 'ReverseETL'
    ui_color = "#f0ede4"
