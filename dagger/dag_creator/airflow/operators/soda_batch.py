from dagger.dag_creator.airflow.operators.awsbatch_operator import AWSBatchOperator

class SodaBatchOperator(AWSBatchOperator):
    custom_operator_name = 'Soda'
    ui_color = "#e4f0e7"
