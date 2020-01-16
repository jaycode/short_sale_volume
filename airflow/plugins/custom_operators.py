from airflow.plugins_manager import AirflowPlugin
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable


class VariableExistenceSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, varnames, reverse=False, *args, **kwargs):
        super(VariableExistenceSensor, self).__init__(*args, **kwargs)
        self.varnames = varnames
        self.reverse = reverse

    def poke(self, context):
        bool_true = True
        bool_false = False
        if self.reverse:
            bool_true = False
            bool_false = True
        status = True
        for varname in self.varnames:
            try:
                var = Variable.get(varname)
                if var != None:
                    status = status and bool_true
                else:
                    status = status and bool_false
            except KeyError as e:
                status = status and bool_false
        return status


# class VariableNonExistenceSensor(BaseSensorOperator):
#     @apply_defaults
#     def __init__(self, varname, *args, **kwargs):
#         super(VariableNonExistenceSensor, self).__init__(*args, **kwargs)
#         self.varname=varname

#     def poke(self, context):
#         try:
#             var = Variable.get(self.varname)
#             if var == None:
#                 return True
#             else:
#                 return False
#         except KeyError as e:
#             return True


# class DataDAGsCreationSensor(BaseSensorOperator):
#     @apply_defaults
#     def __init__(self, *args, **kwargs):
#         return super(DataDAGsCreationSensor, self). \
#             __init__(*args, **kwargs)

#     def poke(self, context):
#         try:
#             short_interest_dag_state = Variable.get("short_interest_dag_state")
#             prices_dag_state = Variable.get("prices_dag_state")
#             if short_interest_dag_state != None and prices_dag_state != None:
#                 return True
#             else:
#                 return False
#         except KeyError as e:
#             return False


# class ETLCompletionSensor(BaseSensorOperator):
#     @apply_defaults
#     def __init__(self, *args, **kwargs):
#         return super(ETLCompletionSensor, self).__init__(*args, **kwargs)

#     def poke(self, context):
#         try:
#             combine_dag_state = Variable.get("combine_dag_state")
#             if combine_dag_state != None:
#                 return True
#             else:
#                 return False
#         except KeyError as e:
#             return False


class CustomOperators(AirflowPlugin):
    name = "custom_operators"
    operators = [VariableExistenceSensor]
