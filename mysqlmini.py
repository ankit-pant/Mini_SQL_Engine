
'''
    Mini Sql Engine
    By: Ankit Pant
'''

import sqlparse
import csv
import sys

data_dict = { }
nj_cond = False
nj_cond2 = False
nj_cond3 = False
nj_cond4 = False


# Get metadata/schema of the tables
def GetSchema():
    '''
    This method is used to get the schema of the tables that are a part of the database
    '''
    table1_attr = []
    table2_attr = []
    with open("./metadata.txt", 'r') as mfile:
        attr = mfile.readline()
        while (attr!="table1\n"):
            attr = mfile.readline()
        while(attr!="<end_table>\n"):
            attr = mfile.readline()
            if attr=="<end_table>\n":
                break
            table1_attr.append(attr)
        attr = mfile.readline()
        while (attr!="table2\n"):
            attr = mfile.readline()
        while(attr!="<end_table>\n"):
            attr = mfile.readline()
            if attr=="<end_table>":
                break
            table2_attr.append(attr)

    for iter in range(0,len(table1_attr)):
        table1_attr[iter] = table1_attr[iter][0:-1]
    for iter in range(0,len(table2_attr)):
        table2_attr[iter] = table2_attr[iter][0:-1]

    return [table1_attr, table2_attr]



def GetRecords(table_name):
    '''
    This method is used to get the data from the tables into memory
    '''
    with open(table_name) as tab:
        recs = []
        csvreader = csv.reader(tab,delimiter=',',quoting=csv.QUOTE_NONE)
        for row in csvreader:
            recs.append(row)

    return recs



def Parse_Conditions(conditions, all_recs, tableno):
    '''
    This method parses conditions to be applied in the query
    '''
    rel_oprs = ['<', '>', "=", '<=', '>=']
    first_cond = ''
    second_cond = ''
    compounder = ''
    conditions_len = len(conditions)

    # Compound Condition
    if conditions_len == 3:
        first_cond = conditions[0]
        compounder = conditions[1]
        second_cond = conditions[2]
        
        # parsing first condition
        attr_name1 = ''
        opr1 = ''
        val1 = ''
        i = 0
        for ch in first_cond:
            if ch in rel_oprs:
                break
            attr_name1 += ch
            i +=1
        opr1 = first_cond[i]
        i += 1
        if first_cond[i] in rel_oprs:
            opr1 += first_cond[i]
            i += 1
        for j in range (i,len(first_cond)):
            val1 += first_cond[j]

        if len(attr_name1) == 1:
            if attr_name1.upper() =='A':
                attr_name1 = 'table1.A'
            if attr_name1.upper() =='C':
                attr_name1 = 'table1.C'
            if attr_name1.upper() =='D':
                attr_name1 = 'table2.D'
            if attr_name1.upper() =='B' and tableno == 1:
                attr_name1 = 'table1.B'
            if attr_name1.upper() =='B' and tableno == 2:
                attr_name1 = 'table2.B'

        # parsing second condition
        attr_name2 = ''
        opr2 = ''
        val2 = ''
        i = 0
        for ch in second_cond:
            if ch in rel_oprs:
                break
            attr_name2 += ch
            i +=1
        opr2 = second_cond[i]
        i += 1
        if second_cond[i] in rel_oprs:
            opr2 += second_cond[i]
            i += 1
        for j in range (i,len(second_cond)):
            val2 += second_cond[j]

        if len(attr_name2) == 1:
            if attr_name2.upper() =='A':
                attr_name2 = 'table1.A'
            if attr_name2.upper() =='C':
                attr_name2 = 'table1.C'
            if attr_name2.upper() =='D':
                attr_name2 = 'table2.D'
            if attr_name2.upper() =='B' and tableno == 1:
                attr_name2 = 'table1.B'
            if attr_name2.upper() =='B' and tableno == 2:
                attr_name2 = 'table2.B'

        # Cases for single table or join
        all_records = []
        for rec in all_recs:
            all_records.append(rec)

        

        if tableno == 1:
            limited_attr1 = data_dict[attr_name1]
            limited_attr2 = data_dict[attr_name2]

            # limiting by opr1
            if opr1 == '<':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr1]) < int(val1):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr
        
            elif opr1 == '>':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr1]) > int(val1):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr
            
            elif opr1 == '=':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr1]) == int(val1):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr
            
            elif opr1 == '<=':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr1]) <= int(val1):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr
            
            elif opr1 == '>=':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr1]) >= int(val1):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr

            else:
                print("Operator not supported!")
                all_recs = []
                return all_recs

            # limiting by opr2 for AND
            if compounder.upper() == "AND":
                if opr2 == '<':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr2]) < int(val2):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
            
                elif opr2 == '>':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr2]) > int(val2):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
                
                elif opr2 == '=':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr2]) == int(val2):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
                
                elif opr2 == '<=':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr2]) <= int(val2):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
                
                elif opr2 == '>=':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr2]) >= int(val2):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr

                else:
                    print("Operator not supported!")
                    all_recs = []
                    return all_recs
            
            elif compounder.upper() == "OR":
                if opr2 == '<':
                    lim_attr = []
                    for itr in  range(0, len(all_records)):
                        if int(all_records[itr][limited_attr2]) < int(val2):
                            lim_attr.append(all_records[itr])
                    for lattr in lim_attr:
                        if lattr not in all_recs:
                            all_recs.append(lattr)
            
                elif opr2 == '>':
                    lim_attr = []
                    for itr in  range(0, len(all_records)):
                        if int(all_records[itr][limited_attr2]) > int(val2):
                            lim_attr.append(all_records[itr])
                    for lattr in lim_attr:
                        if lattr not in all_recs:
                            all_recs.append(lattr)
                
                elif opr2 == '=':
                    lim_attr = []
                    for itr in  range(0, len(all_records)):
                        if int(all_records[itr][limited_attr2]) == int(val2):
                            lim_attr.append(all_records[itr])
                    for lattr in lim_attr:
                        if lattr not in all_recs:
                            all_recs.append(lattr)
                
                elif opr2 == '<=':
                    lim_attr = []
                    for itr in  range(0, len(all_records)):
                        if int(all_records[itr][limited_attr2]) <= int(val2):
                            lim_attr.append(all_records[itr])
                    for lattr in lim_attr:
                        if lattr not in all_recs:
                            all_recs.append(lattr)
                
                elif opr2 == '>=':
                    lim_attr = []
                    for itr in  range(0, len(all_records)):
                        if int(all_records[itr][limited_attr2]) >= int(val2):
                            lim_attr.append(all_records[itr])
                    for lattr in lim_attr:
                        if lattr not in all_recs:
                            all_recs.append(lattr)

                else:
                    print("Operator not supported!")
                    all_recs = []
                    return all_recs
                
            else:
                print("Compound Contition of type", compounder,"not supported!")
                all_recs = []
                return all_recs




        elif tableno == 2:
            limited_attr1 = data_dict[attr_name1] - 3
            limited_attr2 = data_dict[attr_name2] - 3

            # limiting by opr1
            if opr1 == '<':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr1]) < int(val1):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr
        
            elif opr1 == '>':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr1]) > int(val1):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr
            
            elif opr1 == '=':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr1]) == int(val1):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr
            
            elif opr1 == '<=':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr1]) <= int(val1):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr
            
            elif opr1 == '>=':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr1]) >= int(val1):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr

            else:
                print("Operator not supported!")
                all_recs = []
                return all_recs

            # limiting by opr2 for AND
            if compounder.upper() == "AND":
                if opr2 == '<':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr2]) < int(val2):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
            
                elif opr2 == '>':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr2]) > int(val2):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
                
                elif opr2 == '=':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr2]) == int(val2):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
                
                elif opr2 == '<=':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr2]) <= int(val2):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
                
                elif opr2 == '>=':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr2]) >= int(val2):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr

                else:
                    print("Operator not supported!")
                    all_recs = []
                    return all_recs
            
            elif compounder.upper() == "OR":
                if opr2 == '<':
                    lim_attr = []
                    for itr in  range(0, len(all_records)):
                        if int(all_records[itr][limited_attr2]) < int(val2):
                            lim_attr.append(all_records[itr])
                    for lattr in lim_attr:
                        if lattr not in all_recs:
                            all_recs.append(lattr)
            
                elif opr2 == '>':
                    lim_attr = []
                    for itr in  range(0, len(all_records)):
                        if int(all_records[itr][limited_attr2]) > int(val2):
                            lim_attr.append(all_records[itr])
                    for lattr in lim_attr:
                        if lattr not in all_recs:
                            all_recs.append(lattr)
                
                elif opr2 == '=':
                    lim_attr = []
                    for itr in  range(0, len(all_records)):
                        if int(all_records[itr][limited_attr2]) == int(val2):
                            lim_attr.append(all_records[itr])
                    for lattr in lim_attr:
                        if lattr not in all_recs:
                            all_recs.append(lattr)
                
                elif opr2 == '<=':
                    lim_attr = []
                    for itr in  range(0, len(all_records)):
                        if int(all_records[itr][limited_attr2]) <= int(val2):
                            lim_attr.append(all_records[itr])
                    for lattr in lim_attr:
                        if lattr not in all_recs:
                            all_recs.append(lattr)
                
                elif opr2 == '>=':
                    lim_attr = []
                    for itr in  range(0, len(all_records)):
                        if int(all_records[itr][limited_attr2]) >= int(val2):
                            lim_attr.append(all_records[itr])
                    for lattr in lim_attr:
                        if lattr not in all_recs:
                            all_recs.append(lattr)

                else:
                    print("Operator not supported!")
                    all_recs = []
                    return all_recs
                
            else:
                print("Compound Contition of type", compounder,"not supported!")
                all_recs = []
                return all_recs

        elif tableno == 3:
            if val1 in data_dict:
                limited_attr1 = data_dict[attr_name1]
                limited_attr2 = data_dict[val1]

                # Natural Join Condition
                if limited_attr1 == limited_attr2 - 2:
                    lim_attr = []
                    for itr in range(0,len(all_recs)):
                        if int(all_recs[itr][limited_attr1]) == int(all_recs[itr][limited_attr2]):
                            temp = all_recs[itr]
                            del temp[limited_attr2]
                            lim_attr.append(temp)
                    all_recs = lim_attr
                    global nj_cond
                    nj_cond = True
                
                # Limiting by operator
                else:
                    if opr1 == '<':
                        lim_attr = []
                        for itr in  range(0, len(all_recs)):
                            if int(all_recs[itr][limited_attr1]) < int(all_recs[itr][limited_attr2]):
                                lim_attr.append(all_recs[itr])
                        all_recs = lim_attr
                    elif opr1 == '>':
                        lim_attr = []
                        for itr in  range(0, len(all_recs)):
                            if int(all_recs[itr][limited_attr1]) > int(all_recs[itr][limited_attr2]):
                                lim_attr.append(all_recs[itr])
                        all_recs = lim_attr
                    elif opr1 == '=':
                        lim_attr = []
                        for itr in  range(0, len(all_recs)):
                            if int(all_recs[itr][limited_attr1]) == int(all_recs[itr][limited_attr2]):
                                lim_attr.append(all_recs[itr])
                        all_recs = lim_attr
                    elif opr1 == '<=':
                        lim_attr = []
                        for itr in  range(0, len(all_recs)):
                            if int(all_recs[itr][limited_attr1]) <= int(all_recs[itr][limited_attr2]):
                                lim_attr.append(all_recs[itr])
                        all_recs = lim_attr
                    elif opr1 == '>=':
                        lim_attr = []
                        for itr in  range(0, len(all_recs)):
                            if int(all_recs[itr][limited_attr1]) >= int(all_recs[itr][limited_attr2]):
                                lim_attr.append(all_recs[itr])
                        all_recs = lim_attr
                    else:
                        print("Operator not supported!")
                        all_recs = []
                        return all_recs

            else:
                limited_attr1 = data_dict[attr_name1]
                if opr1 == '<':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr1]) < int(val1):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
        
                elif opr1 == '>':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr1]) > int(val1):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
                
                elif opr1 == '=':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr1]) == int(val1):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
                
                elif opr1 == '<=':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr1]) <= int(val1):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
                
                elif opr1 == '>=':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr1]) >= int(val1):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr

                else:
                    print("Operator not supported!")
                    all_recs = []
                    return all_recs
            
            # limiting by opr2 from AND
            if compounder.upper() == "AND":
                if val2 in data_dict:
                    limited_attr1 = data_dict[attr_name2]
                    limited_attr2 = data_dict[val2]
                    # Natural Join Condition
                    if limited_attr1 == limited_attr2 - 2:
                        lim_attr = []
                        for itr in range(0,len(all_recs)):
                            if int(all_recs[itr][limited_attr1]) == int(all_recs[itr][limited_attr2]):
                                temp = all_recs[itr]
                                del temp[limited_attr2]
                                lim_attr.append(temp)
                        all_recs = lim_attr
                        global nj_cond2
                        nj_cond2 = True
                    
                    # Limiting by operator
                    else:
                        if opr2 == '<':
                            lim_attr = []
                            for itr in  range(0, len(all_recs)):
                                if int(all_recs[itr][limited_attr1]) < int(all_recs[itr][limited_attr2]):
                                    lim_attr.append(all_recs[itr])
                            all_recs = lim_attr
                        elif opr2 == '>':
                            lim_attr = []
                            for itr in  range(0, len(all_recs)):
                                if int(all_recs[itr][limited_attr1]) > int(all_recs[itr][limited_attr2]):
                                    lim_attr.append(all_recs[itr])
                            all_recs = lim_attr
                        elif opr2 == '=':
                            lim_attr = []
                            for itr in  range(0, len(all_recs)):
                                if int(all_recs[itr][limited_attr1]) == int(all_recs[itr][limited_attr2]):
                                    lim_attr.append(all_recs[itr])
                            all_recs = lim_attr
                        elif opr2 == '<=':
                            lim_attr = []
                            for itr in  range(0, len(all_recs)):
                                if int(all_recs[itr][limited_attr1]) <= int(all_recs[itr][limited_attr2]):
                                    lim_attr.append(all_recs[itr])
                            all_recs = lim_attr
                        elif opr2 == '>=':
                            lim_attr = []
                            for itr in  range(0, len(all_recs)):
                                if int(all_recs[itr][limited_attr1]) >= int(all_recs[itr][limited_attr2]):
                                    lim_attr.append(all_recs[itr])
                            all_recs = lim_attr
                        else:
                            print("Operator not supported!")
                            all_recs = []
                            return all_recs

                else:
                    limited_attr1 = data_dict[attr_name2]
                    if opr2 == '<':
                        lim_attr = []
                        for itr in  range(0, len(all_recs)):
                            if int(all_recs[itr][limited_attr1]) < int(val2):
                                lim_attr.append(all_recs[itr])
                        all_recs = lim_attr
            
                    elif opr2 == '>':
                        lim_attr = []
                        for itr in  range(0, len(all_recs)):
                            if int(all_recs[itr][limited_attr1]) > int(val2):
                                lim_attr.append(all_recs[itr])
                        all_recs = lim_attr
                    
                    elif opr2 == '=':
                        lim_attr = []
                        for itr in  range(0, len(all_recs)):
                            if int(all_recs[itr][limited_attr1]) == int(val2):
                                lim_attr.append(all_recs[itr])
                        all_recs = lim_attr
                    
                    elif opr2 == '<=':
                        lim_attr = []
                        for itr in  range(0, len(all_recs)):
                            if int(all_recs[itr][limited_attr1]) <= int(val2):
                                lim_attr.append(all_recs[itr])
                        all_recs = lim_attr
                    
                    elif opr2 == '>=':
                        lim_attr = []
                        for itr in  range(0, len(all_recs)):
                            if int(all_recs[itr][limited_attr1]) >= int(val2):
                                lim_attr.append(all_recs[itr])
                        all_recs = lim_attr

                    else:
                        print("Operator not supported!")
                        all_recs = []
                        return all_recs

            elif compounder.upper() == "OR":
                if val2 in data_dict:
                    limited_attr1 = data_dict[attr_name2]
                    limited_attr2 = data_dict[val2]

                    # Natural Join Condition
                    if limited_attr1 == limited_attr2 - 2:
                        lim_attr = []
                        for itr in range(0,len(all_records)):
                            if int(all_records[itr][limited_attr1]) == int(all_records[itr][limited_attr2]):
                                temp = all_records[itr]
                                del temp[limited_attr2]
                                lim_attr.append(temp)
                        for lattr in lim_attr:
                            if lattr not in all_recs:
                                all_recs.append(lattr)
                        global nj_cond3
                        nj_cond3 = True
                    
                    # Limiting by operator
                    else:
                        if opr2 == '<':
                            lim_attr = []
                            for itr in  range(0, len(all_records)):
                                if int(all_records[itr][limited_attr1]) < int(all_records[itr][limited_attr2]):
                                    lim_attr.append(all_records[itr])
                            for lattr in lim_attr:
                                if lattr not in all_recs:
                                    all_recs.append(lattr)
                        elif opr2 == '>':
                            lim_attr = []
                            for itr in  range(0, len(all_records)):
                                if int(all_records[itr][limited_attr1]) > int(all_records[itr][limited_attr2]):
                                    lim_attr.append(all_records[itr])
                            for lattr in lim_attr:
                                if lattr not in all_recs:
                                    all_recs.append(lattr)
                        elif opr2 == '=':
                            lim_attr = []
                            for itr in  range(0, len(all_records)):
                                if int(all_records[itr][limited_attr1]) == int(all_records[itr][limited_attr2]):
                                    lim_attr.append(all_records[itr])
                            for lattr in lim_attr:
                                if lattr not in all_recs:
                                    all_recs.append(lattr)
                        elif opr2 == '<=':
                            lim_attr = []
                            for itr in  range(0, len(all_records)):
                                if int(all_records[itr][limited_attr1]) <= int(all_records[itr][limited_attr2]):
                                    lim_attr.append(all_records[itr])
                            for lattr in lim_attr:
                                if lattr not in all_recs:
                                    all_recs.append(lattr)
                        elif opr2 == '>=':
                            lim_attr = []
                            for itr in  range(0, len(all_records)):
                                if int(all_records[itr][limited_attr1]) >= int(all_records[itr][limited_attr2]):
                                    lim_attr.append(all_records[itr])
                            for lattr in lim_attr:
                                if lattr not in all_recs:
                                    all_recs.append(lattr)
                        else:
                            print("Operator not supported!")
                            all_recs = []
                            return all_recs

                else:
                    limited_attr1 = data_dict[attr_name2]
                    if opr2 == '<':
                        lim_attr = []
                        for itr in  range(0, len(all_records)):
                            if int(all_records[itr][limited_attr1]) < int(val2):
                                lim_attr.append(all_records[itr])
                        for lattr in lim_attr:
                            if lattr not in all_recs:
                                all_recs.append(lattr)
            
                    elif opr2 == '>':
                        lim_attr = []
                        for itr in  range(0, len(all_records)):
                            if int(all_records[itr][limited_attr1]) > int(val2):
                                lim_attr.append(all_records[itr])
                        for lattr in lim_attr:
                            if lattr not in all_recs:
                                all_recs.append(lattr)
                    
                    elif opr2 == '=':
                        lim_attr = []
                        for itr in  range(0, len(all_recs)):
                            if int(all_records[itr][limited_attr1]) == int(val2):
                                lim_attr.append(all_records[itr])
                        for lattr in lim_attr:
                            if lattr not in all_recs:
                                all_recs.append(lattr)
                    
                    elif opr2 == '<=':
                        lim_attr = []
                        for itr in  range(0, len(all_recs)):
                            if int(all_records[itr][limited_attr1]) <= int(val2):
                                lim_attr.append(all_records[itr])
                        for lattr in lim_attr:
                            if lattr not in all_recs:
                                all_recs.append(lattr)
                    
                    elif opr2 == '>=':
                        lim_attr = []
                        for itr in  range(0, len(all_recs)):
                            if int(all_records[itr][limited_attr1]) >= int(val2):
                                lim_attr.append(all_records[itr])
                        for lattr in lim_attr:
                            if lattr not in all_recs:
                                all_recs.append(lattr)

                    else:
                        print("Operator not supported!")
                        all_recs = []
                        return all_recs



    # Simple Condition
    else:
        first_cond = conditions[0]

        # parsing first condition
        attr_name = ''
        opr = ''
        val = ''
        i = 0
        for ch in first_cond:
            if ch in rel_oprs:
                break
            attr_name += ch
            i +=1
        opr = first_cond[i]
        i += 1
        if first_cond[i] in rel_oprs:
            opr += first_cond[i]
            i += 1
        for j in range (i,len(first_cond)):
            val += first_cond[j]


        if len(attr_name) == 1:
            if attr_name.upper() =='A':
                attr_name = 'table1.A'
            if attr_name.upper() =='C':
                attr_name = 'table1.C'
            if attr_name.upper() =='D':
                attr_name = 'table2.D'
            if attr_name.upper() =='B' and tableno == 1:
                attr_name = 'table1.B'
            if attr_name.upper() =='B' and tableno == 2:
                attr_name = 'table2.B'
        
        # Condition on attribute
        if val in data_dict:
            limited_attr1 = data_dict[attr_name]
            limited_attr2 = data_dict[val]

            # Natural Join Condition
            if limited_attr1 == limited_attr2 - 2:
                lim_attr = []
                for itr in range(0,len(all_recs)):
                    if int(all_recs[itr][limited_attr1]) == int(all_recs[itr][limited_attr2]):
                        temp = all_recs[itr]
                        del temp[limited_attr2]
                        lim_attr.append(temp)
                all_recs = lim_attr
                global nj_cond4
                nj_cond4 = True
            
            # Limiting by operator
            else:
                if opr == '<':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr1]) < int(all_recs[itr][limited_attr2]):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
                elif opr == '>':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr1]) > int(all_recs[itr][limited_attr2]):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
                elif opr == '=':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr1]) == int(all_recs[itr][limited_attr2]):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
                elif opr == '<=':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr1]) <= int(all_recs[itr][limited_attr2]):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
                elif opr == '>=':
                    lim_attr = []
                    for itr in  range(0, len(all_recs)):
                        if int(all_recs[itr][limited_attr1]) >= int(all_recs[itr][limited_attr2]):
                            lim_attr.append(all_recs[itr])
                    all_recs = lim_attr
                else:
                    print("Operator not supported!")
                    all_recs = []
                    return all_recs

            
        # No join conditions
        else:

            limited_attr = data_dict[attr_name]
            if (tableno==2):
                limited_attr -= 3

            # limiting by opr
            if opr == '<':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr]) < int(val):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr
            
            elif opr == '>':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr]) > int(val):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr
            
            elif opr == '=':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr]) == int(val):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr
            
            elif opr == '<=':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr]) <= int(val):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr
            
            elif opr == '>=':
                lim_attr = []
                for itr in  range(0, len(all_recs)):
                    if int(all_recs[itr][limited_attr]) >= int(val):
                        lim_attr.append(all_recs[itr])
                all_recs = lim_attr

            else:
                print("Operator not supported!")
                all_recs = []

    return all_recs




def Print_Output(attrs_to_print, all_recs, aggr, tableno, table1_attr, table2_attr):
    '''
    This method outputs the result of the query
    '''

    # Handling empty output
    if len(all_recs) == 0:
        print("No records found for the given criterion!")
        return

    # Handling select all records
    if len(attrs_to_print)==1 and attrs_to_print[0] == '*':
        if(tableno == 1):
            for attrs in table1_attr:
                if attrs == table1_attr[-1]:
                    print("table1.",attrs,sep='',end='\n')
                else:
                    print("table1.",attrs,sep='',end=', ')
            for row in all_recs:
                for at in row:
                    if at == row[-1]:
                        print(at, end='')
                    else:
                        print(at, end=', ')
                print()
        elif(tableno == 2):
            for attrs in table2_attr:
                if attrs == table2_attr[-1]:
                    print("table2.",attrs,sep='',end='\n')
                else:
                    print("table2.",attrs,sep='',end=', ')
            for row in all_recs:
                for at in row:
                    if at == row[-1]:
                        print(at, end='')
                    else:
                        print(at, end=', ')
                print()
                
        if(tableno == 3):
            for attrs in table1_attr:
                print("table1.",attrs,sep='',end=', ')
            for attrs in table2_attr:
                if (nj_cond == True or nj_cond2 == True or nj_cond3 == True or nj_cond4 == True) and attrs == "B":
                    continue
                if attrs == table2_attr[-1]:
                    print("table2.",attrs,sep='',end='\n')
                else:
                    print("table2.",attrs,sep='',end=', ')
            for row in all_recs:
                for at in row:
                    if at == row[-1]:
                        print(at, end='')
                    else:
                        print(at, end=', ')
                print()

    # Handling printing of specific attributes
    else:
        if(tableno == 1):
            if aggr == 1:
                attr = attrs_to_print[0]
                print("SUM(table1.",table1_attr[attr],')', sep='')
                attr_sum = 0
                for row in all_recs:
                    attr_sum += int(row[attr])
                print(attr_sum)
            elif aggr == 2:
                attr = attrs_to_print[0]
                print("AVG(table1.",table1_attr[attr],')', sep='')
                attr_sum = 0
                i = 0
                for row in all_recs:
                    attr_sum += int(row[attr])
                    i += 1
                print(attr_sum/i)
            elif aggr == 3:
                attr = attrs_to_print[0]
                print("MAX(table1.",table1_attr[attr],')', sep='')
                attr_max = -sys.maxsize -1
                for row in all_recs:
                    attr_max = max(attr_max, int(row[attr]))
                print(attr_max)
            elif aggr == 4:
                attr = attrs_to_print[0]
                print("MIN(table1.",table1_attr[attr],')', sep='')
                attr_min = sys.maxsize
                for row in all_recs:
                    attr_min = min(attr_min, int(row[attr]))
                print(attr_min)
            elif aggr == 5:
                attr = attrs_to_print[0]
                print("DISTINCT(table1.",table1_attr[attr],')', sep='')
                distinct_attrs = set()
                for row in all_recs:
                    distinct_attrs.add(int(row[attr]))
                for el in distinct_attrs:
                    print(el)
            else:
                for attrs in attrs_to_print:
                    if attrs == attrs_to_print[-1]:
                        print("table1.",table1_attr[int(attrs)],sep='',end='\n')
                    else:
                        print("table1.",table1_attr[int(attrs)],sep='',end=', ')

                for row in all_recs:
                    for attr in attrs_to_print:
                        if attr == attrs_to_print[-1]:
                            print(row[attr], sep='',end='\n')
                        else:
                            print(row[attr], sep='',end=', ')
                

        if(tableno == 2):
            if aggr == 1:
                attr = attrs_to_print[0] - 3
                print("SUM(table2.",table2_attr[attr],')', sep='')
                attr_sum = 0
                for row in all_recs:
                    attr_sum += int(row[attr])
                print(attr_sum)
            elif aggr == 2:
                attr = attrs_to_print[0] - 3
                print("AVG(table2.",table2_attr[attr],')', sep='')
                attr_sum = 0
                i = 0
                for row in all_recs:
                    attr_sum += int(row[attr])
                    i += 1
                print(attr_sum/i)
            elif aggr == 3:
                attr = attrs_to_print[0] - 3
                print("MAX(table2.",table2_attr[attr],')', sep='')
                attr_max = -sys.maxsize -1
                for row in all_recs:
                    attr_max = max(attr_max, int(row[attr]))
                print(attr_max)
            elif aggr == 4:
                attr = attrs_to_print[0] - 3
                print("MIN(table2.",table2_attr[attr],')', sep='')
                attr_min = sys.maxsize
                for row in all_recs:
                    attr_min = min(attr_min, int(row[attr]))
                print(attr_min)
            elif aggr == 5:
                attr = attrs_to_print[0] - 3
                print("DISTINCT(table2.",table2_attr[attr],')', sep='')
                distinct_attrs = set()
                for row in all_recs:
                    distinct_attrs.add(int(row[attr]))
                for el in distinct_attrs:
                    print(el)
            else:
                for attrs in attrs_to_print:
                    attrs = attrs - 3
                    if attrs == attrs_to_print[-1] - 3:
                        print("table2.",table2_attr[int(attrs)],sep='',end='\n')
                    else:
                        print("table2.",table2_attr[int(attrs)],sep='',end=', ')

                for row in all_recs:
                    for attr in attrs_to_print:
                        attr = attr - 3
                        if attr == attrs_to_print[-1] - 3:
                            print(row[attr], sep='',end='\n')
                        else:
                            print(row[attr], sep='',end=', ')

        if(tableno == 3):
            if aggr == 1:
                attr = attrs_to_print[0]
                if attr <=2:
                    msg = "SUM(table1."
                    tab = table1_attr[attr]
                else:
                    msg = "SUM(table2."
                    tab = table2_attr[attr-3]
                print(msg,tab,')', sep='')
                attr_sum = 0
                for row in all_recs:
                    attr_sum += int(row[attr])
                print(attr_sum)
            elif aggr == 2:
                attr = attrs_to_print[0]
                if attr <=2:
                    msg = "AVG(table1."
                    tab = table1_attr[attr]
                else:
                    msg = "AVG(table2."
                    tab = table2_attr[attr-3]
                print(msg,tab,')', sep='')
                attr_sum = 0
                i = 0
                for row in all_recs:
                    attr_sum += int(row[attr])
                    i += 1
                print(attr_sum/i)
            elif aggr == 3:
                attr = attrs_to_print[0]
                if attr <=2:
                    msg = "MAX(table1."
                    tab = table1_attr[attr]
                else:
                    msg = "MAX(table2."
                    tab = table2_attr[attr-3]
                print(msg,tab,')', sep='')
                attr_max = -sys.maxsize -1
                for row in all_recs:
                    attr_max = max(attr_max, int(row[attr]))
                print(attr_max)
            elif aggr == 4:
                attr = attrs_to_print[0]
                if attr <=2:
                    msg = "MIN(table1."
                    tab = table1_attr[attr]
                else:
                    msg = "MIN(table2."
                    tab = table2_attr[attr-3]
                print(msg,tab,')', sep='')
                attr_min = sys.maxsize
                for row in all_recs:
                    attr_min = min(attr_min, int(row[attr]))
                print(attr_min)
            elif aggr == 5:
                attr = attrs_to_print[0]
                if attr <=2:
                    msg = "DISTINCT(table1."
                    tab = table1_attr[attr]
                else:
                    msg = "DISTINCT(table2."
                    tab = table2_attr[attr-3]
                print(msg,tab,')', sep='')
                distinct_attrs = set()
                for row in all_recs:
                    distinct_attrs.add(int(row[attr]))
                for el in distinct_attrs:
                    print(el)

            else:
                for attrs in attrs_to_print:
                    if attrs > 2:
                        continue
                    if attrs == attrs_to_print[-1]:
                        print("table1.",table1_attr[int(attrs)],sep='',end='\n')
                    else:
                        print("table1.",table1_attr[int(attrs)],sep='',end=', ')
                for attrs in attrs_to_print:
                    if attrs < 3:
                        continue
                    attrs = attrs - 3
                    if attrs == attrs_to_print[-1] - 3:
                        print("table2.",table2_attr[int(attrs)],sep='',end='\n')
                    else:
                        print("table2.",table2_attr[int(attrs)],sep='',end=', ')
                for row in all_recs:
                    for attr in attrs_to_print:
                        if (nj_cond == True or nj_cond2 == True or nj_cond3 == True or nj_cond4 == True):
                            if attr > 3:
                                attr -= 1
                        if attr == attrs_to_print[-1]:
                            print(row[attr], sep='',end='\n')
                        elif (nj_cond == True or nj_cond2 == True or nj_cond3 == True or nj_cond4 == True) and attr == attrs_to_print[-1] -1 :
                            print(row[attr], sep='',end='\n')
                        else:
                            print(row[attr], sep='',end=', ')




def Process_Query(attrs, tables, conditions, table1_attr, table1_recs, table2_attr, table2_recs):
    '''
    This method prints the output from the processed query
    '''
    all_tables = False
    aggr = 0
    # Checking for attributes to print
    attrs_to_print = []
    if len(attrs)==1:
        if attrs[0]=='*':
            attrs_to_print = ['*']
        else:
            if 'SUM' in attrs[0].upper():
                aggr = 1
                ats = attrs[0].split('(')
                attrs_to_print.append(data_dict[ats[1][0:-1]])
            elif 'AVG' in attrs[0].upper():
                aggr = 2
                ats = attrs[0].split('(')
                attrs_to_print.append(data_dict[ats[1][0:-1]])
            elif 'MAX' in attrs[0].upper():
                aggr = 3
                ats = attrs[0].split('(')
                attrs_to_print.append(data_dict[ats[1][0:-1]])
            elif 'MIN' in attrs[0].upper():
                aggr = 4
                ats = attrs[0].split('(')
                attrs_to_print.append(data_dict[ats[1][0:-1]])
            elif 'DISTINCT' in attrs[0].upper():
                aggr = 5
                ats = attrs[0].split('(')
                attrs_to_print.append(data_dict[ats[1][0:-1]])
            elif attrs[0] not in data_dict:
                print("Error! Attribute not present in Table...")
                return
            else:
                attrs_to_print.append(data_dict[attrs[0]])
    else:
        for attr in attrs:
            # print(attr)
            if attr not in data_dict:
                print("Error! Attribute \"", attr, "\" not present in Table...")
                return
            else:
                attrs_to_print.append(data_dict[attr])

    
    # Handling the case with Cartesian product
    if len(tables)==2:
        all_tables = True
    all_recs = []
    # all_recs.append(table1_recs[0])
    # all_recs[0].extend(table2_recs[0])
    # i = 0
    if all_tables and (tables[0].upper() == "TABLE1" and tables[1].upper() == "TABLE2" or tables[0].upper() == "TABLE2" and tables[1].upper() == "TABLE1"):
        for r1 in table1_recs:
            for r2 in table2_recs:
                temp = []
                for r in r1:
                    temp.append(r)
                for r in r2:
                    temp.append(r)
                all_recs.append(temp)
        tableno = 3
    elif tables[0].upper() == "TABLE1":
        all_recs = table1_recs
        tableno = 1
    elif tables[0].upper() == "TABLE2":
        all_recs = table2_recs
        tableno = 2
    else:
        print("Error! Table specified in query not found in Database...")
        return

    # Handling conditions in query
    if len(conditions)!=0:
        all_recs = Parse_Conditions(conditions, all_recs, tableno)
        # TODO: Modify records to be printed based on condition
    
    Print_Output(attrs_to_print, all_recs, aggr, tableno, table1_attr, table2_attr)




def Parse_Query(q, table1_attr, table1_recs, table2_attr, table2_recs):
    '''
    This method processes each query
    '''
    query = q.split(' ')

    # Handling semicolon at end if present
    if(query[-1][-1]==';'):
        query[-1] = query[-1][0:-1]

    querylen = len(query)
    operation = query[0]
    if(operation.upper() != "SELECT"):
        print("Error! only SELECT statements are supported currently...")
        return

    # Getting attributes to print
    attrs = []
    for prse in range(1,querylen):
        att = query[prse]
        if(att.upper()=="FROM"):
            break
        attrs.extend(att.split(','))


    # Removing empty attributes
    i = 0
    for att in attrs:
        if(len(attrs[i])==0):
            del attrs[i]
        i+=1

    # Checking if attributes requested
    if len(attrs) == 0:
        print("Error! No attribute to display mentioned...")
        return

    # Getting  tables to select from
    tables = []
    strt = prse + 1
    for prse in range(strt,querylen):
        tab = query[prse]
        if(tab.upper()=="WHERE"):
            break
        tables.extend(tab.split(','))
    
    # Removing empty tables
    i = 0
    for tab in tables:
        if(len(tables[i])==0):
            del tables[i]
        i+=1

    # Checking if tables present in query
    if len(tables) == 0:
        print("Error! No tables specified in query...")
        return
    if len(tables) > 2:
        print("Error! Too many tables in query...")

    # Creating uniform attribute names
    for i in range (0,len(attrs)):
        if attrs[i].upper()=='A': # and (tables[0].upper():
            attrs[i] = "table1.A"
        elif attrs[i].upper()=='C': # and tables[0].upper()=='TABLE1'):
            attrs[i] = "table1.C"
        elif attrs[i].upper()=='D': # and tables[0].upper()=='TABLE2'):
            attrs[i] = "table2.D"
        elif attrs[i].upper()=='B'  and tables[0].upper()=='TABLE1':
            attrs[i] = "table1.B"
        elif attrs[i].upper()=='B'  and tables[0].upper()=='TABLE2':
            attrs[i] = "table2.B"

    # Getting the conditions
    conditions = []
    strt = prse + 1

    # Checking if conditions present in query
    if query[prse].upper()=="WHERE" and strt == querylen:
        print("Error! No condition specified in query after where clause...")
        return

    for prse in range(strt,querylen):
        cond = query[prse]
        conditions.extend(cond.split(' '))
    
    # Printing the entered query
    # print("SELECT",attrs)
    # print("FROM",tables)
    # if len(conditions) != 0:
    #     print("WHERE", conditions)
    Process_Query(attrs, tables, conditions, table1_attr, table1_recs, table2_attr, table2_recs)



if __name__ == "__main__":

    # Get metadata/schema of the tables
    table1_attr = GetSchema()[0]
    table2_attr = GetSchema()[1]

    # Creating data dictionary from read schema
    idx = 0
    for attr in table1_attr:
        at = "table1." + attr
        data_dict[at] = idx
        at = "TABLE1." + attr
        data_dict[at] = idx
        idx += 1
    for attr in table2_attr:
        at = "table2." + attr
        data_dict[at] = idx
        at = "TABLE2." + attr
        data_dict[at] = idx
        idx += 1

    # print(data_dict)

    # Check for proper number of command line arguments
    if len(sys.argv)<2:
        print("Please provide the sql query")
        print("Usage: python 2018201035.py \"query1; query2; ... queryn\"")
        print("Program will exit...")
        exit(-1)
    
    if len(sys.argv)>2:
        print("Please provide only one argument")
        print("Usage: python 2018201035.py \"query1; query2; ... queryn\"")
        print("Program will exit...")
        exit(-1)

    args = sys.argv[1]
    queries = sqlparse.split(args)
    
    # Get records of tables
    path1 = "./table1.csv"
    path2 = "./table2.csv"
    table1_recs = GetRecords(path1)
    table2_recs = GetRecords(path2)

    for query in queries:
        # print("Output of query:")
        print()
        Parse_Query(query, table1_attr, table1_recs, table2_attr, table2_recs)
        print()
    
