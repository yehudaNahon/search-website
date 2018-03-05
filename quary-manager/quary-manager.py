#!/usr/bin/env python3

import re

operations = {
    'not': (lambda first, second: 
            [word for word in first if word not in second]),
    'or': (lambda first, second: first + list(set(second) - set(first))),
    'and': (lambda first, second: [word for word in first if word in second])
}

word_re = re.compile("^([a-zA-Z1-9]+)$")
complex_quary_re = re.compile("([a-zA-Z1-9]+|\(.+\))\s+(" + '|'.join(operations.keys()) + ")\s+([a-zA-Z1-9]+|\(.+\))")
expr_pattern = "\(.+\)"


def CreateTree(quary, word_dict):    
    quary = quary.strip()

    # check if the quary is a single word quary exmp : "word"
    if word_re.match(quary):
        word = word_re.findall(quary)[0]
        if word in word_dict:
            return word_dict[word]
        else:
            return None

    if complex_quary_re.match(quary):
        first_exp, operation, second_exp = complex_quary_re.findall(quary)[0]
        
        if re.match(expr_pattern, first_exp):
            first_lst = CreateTree(first_exp[1:-1], word_dict)
        else:
            first_lst = CreateTree(first_exp, word_dict)

        if re.match(expr_pattern, second_exp):
            second_lst = CreateTree(second_exp[1:-1], word_dict)
        else:
            second_lst = CreateTree(second_exp, word_dict)

        if type(first_lst) is list and operation in operations and type(second_lst) is list:
            return operations[operation](first_lst, second_lst)
        else:
            return None

if __name__ == '__main__':

    quary = " (this and (is or this)) or (where and are)"
    words = {'this': [0,1,2,3], 'is': [0,1,3], 'where' : [4,5,6,7], 'are': [5,6]}

    answer = CreateTree(quary, words)

    print(answer)