#!/usr/bin/python
# -*- coding: utf-8 -*-
#
###
from castor.config import CassandraKeyspace
from cassandra import InvalidRequest
import re


class Operation:

    def add(stack):
        x = stack.pop()
        y = stack.pop()
        return x+y

    def minus(stack):
        x = stack.pop()
        y = stack.pop()
        return y-x

    def div(stack):
        x = stack.pop()
        y = stack.pop()
        return y/x

    def mult(stack):
        x = stack.pop()
        y = stack.pop()
        return x*y

    def getmax(stack):
        x = stack.pop()
        y = stack.pop()
        return max(x, y)

    def variable_defined(stack):
        substitution = stack.pop()
        evaluated_variable = stack.pop()
        if evaluated_variable:
            return substitution
        return float(0)
   
    def variable_undefined(stack):
        substitution = stack.pop()
        evaluated_variable = stack.pop()
        if evaluated_variable is None:
            return substitution
        return evaluated_variable
    
    def is_undef(stack):
        return stack.pop() is None

    def if_condition(stack):
        is_false = stack.pop()
        is_true = stack.pop()
        condition = stack.pop()
        if condition:
            return is_true
        else:
            return is_false

    @staticmethod
    def compare(stack, op):
        y = stack.pop()
        x = stack.pop()
        return op(x,y)

    def is_gt(stack): return Operation.compare(stack, lambda x,y: x>y)
    def is_ge(stack): return Operation.compare(stack, lambda x,y: x>=y)
    def is_lt(stack): return Operation.compare(stack, lambda x,y: x<y)
    def is_le(stack): return Operation.compare(stack, lambda x,y: x<=y)


    operations = { '+': add,'-': minus, '/': div, '*': mult, 'MAX': getmax, 'V_DEF': variable_defined,
            'V_UNDEF': variable_undefined, 'UN': is_undef, 'IF': if_condition,
            'GT': is_gt, 'LT': is_lt, 'GE': is_ge, 'LE': is_le
            }
    

    class BadRPNException(Exception):
        pass
    
    class NoDataSetException(Exception):
        pass


    class StackError(Exception):
        pass


    class HostNeededError(Exception):
        pass

    def __init__(self, rpn_expr):
        self.const = re.compile('^-?\d+(\.\d+)?$')
        rpn_array = rpn_expr.split(',')
        #datasets need to be interpolated before calling method evaluate
        self.datasets_interpolated = False
        self.datasets = {}
        self.rpn = []
        self.variables = []
        for elem in rpn_array:
            is_const = self.const.match(elem)
            if is_const:
                self.rpn.append(float(elem))
            elif Operation.operations.has_key(elem):
                self.rpn.append(elem)
            else:
                #it is a variable
                #variable name can finish with  UNDEF:constant or DEF:constant (default value defined)
                #character : is allowed in variable name, so we split variable name with : and we reappend
                #elements before UNDEF or DEF

                variable_elements = elem.split(':')
                clause_index = None
                if 'UNDEF' in variable_elements:
                    clause_index = variable_elements.index('UNDEF')
                if 'DEF' in variable_elements:
                    clause_index = variable_elements.index('DEF')
                
                if clause_index is not None:
                    if clause_index == 0 or clause_index == len(variable_elements):
                        raise BadRPNException("Variable %s For expression %s"%(elem, rpn_expr))

                    variable_name = ':'.join(variable_elements[:clause_index])
                    self.variables.append(variable_name)
                    self.rpn.append(variable_name)
                    self.rpn.append(float(variable_elements[clause_index+1]))
                    self.rpn.append('V_%s'%variable_elements[clause_index])
                else:
                    variable_name = elem
                    self.variables.append(variable_name)
                    self.rpn.append(variable_name)



    def get_variables(self):
        return self.variables

    def set_dataset(self,name, dataset):
        self.datasets_interpolated = False
        self.datasets[name] = dataset


    def __interpolate_datasets__(self):
        """
        until the smallest step of all datasets exceed required_step + 30% we multiply required_step by two
        (only one value on two value is taken) and method interpolate is called on dataset
        """
        self.datasets_interpolated = True
        first_dataset = self.datasets[self.datasets.keys()[0]]
        required_step = first_dataset.get_step()
        start = first_dataset.get_first_ts()
        end =  first_dataset.get_last_ts()
        if required_step is None:
            return (start, None, end)
        min_real_step = min(first_dataset.get_raw_mean_step(), 1800)
        #sometimes we can have a big hole of data, and so get_raw_mean_step will be enormous
        #and so it's more desirable to have a graph with a step of 1800
        for dataset in self.datasets.values():
            if dataset.get_raw_mean_step() < min_real_step:
                min_real_step = dataset.get_raw_mean_step()

        while min_real_step > (required_step * 1.3):
            """
            if the most precise real step for all datasets is less precise than required_step
            we need to augment required_step. We will multiply it by two. (==> Only only one value on two will be taken)
            to avoid to create unused points
            """
            required_step = required_step*2

        for dataset in self.datasets.values():
            dataset.interpolate(required_step)
        return(start,required_step,end)


    def get_available_ts(self):
        """
        Returns timestamps avaibable
        If required step is 30 but all datasets have only a point each 60s 1385430:5 1385490:None 1385520:10 1385590:None
        this function function will return an iterator giving a step of 60
        """
        (start, consolidated_step, end) = self.__interpolate_datasets__()
        if start is None:
            return
        if consolidated_step is None:
            first_dataset = self.datasets[self.datasets.keys()[0]] 
            for k in first_dataset.get_data().keys():
                yield k            
        else:
            for i in range(start,end+1,consolidated_step):
                yield i


    def evaluate(self, timestamp, consolidation_func='AVG'):
        if not self.datasets_interpolated:
            self.__interpolate_datasets__()
        stack = []
        for element in self.rpn:
            if Operation.operations.has_key(element):
                try:
                    stack.append(Operation.operations[element](stack))
                except ZeroDivisionError:
                    stack.append(None)
                except IndexError:
                    raise Operation.StackError("Empty stack evaluating %s at %s"%(self.rpn, element))
                except TypeError:
                    stack.append(None)
            elif self.const.match(str(element)):
                stack.append(element)
            else:
                #its a variable
                if self.datasets.has_key(element):
					extracted_value = None
					try:
						extracted_value = self.datasets[element][timestamp][consolidation_func]
					except KeyError:
						pass
 					stack.append(extracted_value)
                else:
                    raise Operation.NoDataSetException("Dataset for %s doesn't exist"%element)
                #print stack
        if len(stack) == 1:
            return stack[0]
        else:
            raise Operation.StackError("Bad number of elements in stack (%s) at the end of RPN expression %s, stack is %s"%(len(stack), self.rpn, stack))

