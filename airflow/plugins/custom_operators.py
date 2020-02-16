from airflow.plugins_manager import AirflowPlugin
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable


class VariableExistenceSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, varnames, reverse=False, operation='AND', *args, **kwargs):
        super(VariableExistenceSensor, self).__init__(*args, **kwargs)
        self.varnames = varnames
        self.reverse = reverse
        self.operation = operation

    def poke(self, context):
        bool_true = True
        bool_false = False
        if self.reverse:
            bool_true = False
            bool_false = True

        if self.operation == 'AND':
            status = True
            for varname in self.varnames:
                var = Variable.get(varname, default_var=None)
                if var != None:
                    status = status and bool_true
                else:
                    status = status and bool_false
        elif self.operation == 'OR':
            status = False
            for varname in self.varnames:
                var = Variable.get(varname, default_var=None)
                if var != None:
                    status = status or bool_true
                else:
                    status = status or bool_false
        return status


class CustomOperators(AirflowPlugin):
    name = "custom_operators"
    operators = [VariableExistenceSensor]
