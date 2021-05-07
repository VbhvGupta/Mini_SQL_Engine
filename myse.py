#################### VAIBHAV GUPTA #######################
####################  2020201040   #######################

import os,sys,re
import collections
import sqlparse
import copy
import csv

schema_of_tables = {}
CONDITIONAL_TUPLE=("AND", "OR", "(", ")" , "and" , "or")
OPERATOR_TUPLE=("=", ">", "<", "!=", "<=", ">=")
final_schema = {}

def display(table,aggregate_functions_map) :
	keys = list(final_schema.keys())
	#print(keys) ####
	fun_dict = {}
	for i in range(len(aggregate_functions_map)):
		(col , fn) = aggregate_functions_map[i]
		fun_dict[fn] = col
	ans=[]
	for k in keys :
		if k.lower() in ("sum" , "average" , "min" , "max" , "count") :
			tmp = str(k.lower())+"("+ str(fun_dict[k]) + ")"
			ans.append(tmp)
		for i in schema_of_tables:
			if k in schema_of_tables[i] :
				tmp = str(i) + "." + str(k)
				ans.append(tmp)
	#print("<",end="")
	print(*ans,sep = ",")
	#print("")
	for row in table :
		print(*row, sep = ",")

def metadatatxt():
    tbl = False
    bt="<begin_table>"
    et="<end_table>"
    with open("metadata.txt","r") as m:
        for line in m:
        	line = line.strip()
        	if line == bt:
        		columns = []
        		tbl = True
        		continue
        	if tbl == True:
        		tablename = line
        		tbl = False
        		continue
        	if line == et:
        		schema_of_tables[tablename] = columns
        		continue
        	columns.append(line)
    #print(schema_of_tables)  ########

def read_table(name):
    result = []
    name = name + ".csv"
    try:
        reader=csv.reader(open(name),delimiter=',')
    except Exception:
        print("ERROR : table not found")
        sys.exit()
    res = list(reader)
    result = []
    for lyn in res :
    	lyn = list(map(int, lyn))
    	result.append(lyn)    
    #print(result)
    return result

def join(table_names):
    table = read_table(table_names[0])
    if len(table_names) == 1:
        return table
    else:
        for table_name in table_names[1:]:
            temp_table = read_table(table_name)
            temp = []
            for row in table:
                for k in range(0,len(temp_table)):
                    temp.append(row+temp_table[k])
            table = temp
    #print(table)
    return table

def get_col_aggr(cols,groupby_flag):
    count = 0
    aggr = []
    aggr_func_map = []
    column = []
    aggr_flag = False
    simple_coloumn_flag = False
    for col in cols:
        c = col.strip()
        #if count > 1 :
        	#print("Multiple Aggregate Functions used")
        	#sys.exit()
        if c.lower()[:3] in ["max", "min", "sum"]:
        	if simple_coloumn_flag == True and groupby_flag == False:
        		print("Both coloumn and Aggregate function not allowed without group by clause")
        		sys.exit()
        	aggr_flag = True
        	count+=1
        	#aggr.append(c.split("(")[0])
        	#column.append(c[4:len(c)-1])
        	aggr_func_map.append( (c[4:len(c)-1] , c.split("(")[0]))
        elif c.lower()[:5] == "count" :
        	if simple_coloumn_flag == True and groupby_flag == False:
        		print("Both coloumn and Aggregate function not allowed without group by clause")
        		sys.exit()        	
        	aggr_flag = True
        	#aggr.append(c.split("(")[0])
            #column.append(c[6:len(c)-1])
        	aggr_func_map.append( (c[6:len(c)-1] , c.split("(")[0]) )
        	count+=1
        elif c.lower()[:7] == "average" :
        	if simple_coloumn_flag == True and groupby_flag == False:
        		print("Both coloumn and Aggregate function not allowed without group by clause")
        		sys.exit()
        	aggr_flag = True
        	#aggr.append(c.split("(")[0])
            #column.append(c[8:len(c)-1])
        	aggr_func_map.append( (c[8:len(c)-1] , c.split("(")[0]) )
        	count+=1
        else :
        	if aggr_flag == True and groupby_flag == False:
        		print("Both coloumn and Aggregate function not allowed without group by clause")
        		sys.exit()
        	simple_coloumn_flag = True
        	column.append(c)
    return column,aggr_func_map	

def compute_conditions(cndition,joined_table,column_dict):
	cndition = cndition.replace("(","( ")
	cndition = cndition.replace(")"," )")
	conditions = cndition.split(" ")
	#print(conditions)  ##
	pattern=re.compile(r'(=|!=|<=|<|>=|>)') 
	temp=[]
	for i in conditions:
		if i.lower() in ("and" , "or"):
			i=i.lower()
		if i.lower() not in CONDITIONAL_TUPLE:
			text=i
			sub_cond=re.sub(pattern,r' \1 ',text) ## replaces matching operator with same operator with surround space
			cond=tuple(sub_cond.split())
		else:
			cond=i
		temp.append(cond)
	conditions=temp
	#print(conditions)
	operator = None
	final_dataset=[]
	for row in joined_table :
		#print(row)
		new_cond=[]
		for condition in conditions :
			if condition in CONDITIONAL_TUPLE:
				new_cond.append(condition.lower())
			else:
				if len(list(condition))==3:
					operator = list(condition)[1]
				elif len(list(condition))>3:
					operator=list(condition)[1]+""+list(condition)[2]
				else:
					print("Something is wrong")
				table_column=list(condition)[0]
				val=int(condition[len(list(condition))-1])
				
				#print(table_column) #####
				#print(val) #####
				#table_column=handling_colname_with_tablename(table_column,tables)
				if (operator=="=" and int(row[column_dict[table_column]])==val) or \
				   (operator==">" and int(row[column_dict[table_column]])>val) or \
				   (operator=="<" and int(row[column_dict[table_column]])<val) or \
				   (operator=="!=" and int(row[column_dict[table_column]])!=val) or \
				   (operator=="<=" and int(row[column_dict[table_column]])<=val) or \
				   (operator==">=" and int(row[column_dict[table_column]])>=val) :
				   new_cond.append("True")
				else :
					if operator not in OPERATOR_TUPLE:
						oprt = []
						for i in range(len(aggregate_functions_map)):
							oprt.append(i)
						print("Invalid operand present")
						sys.exit()
					new_cond.append("False")
		flag = "True"
		if len(new_cond)>0:
			boolean_exp=new_cond[0]
			for z in range(1,len(new_cond)):
				boolean_exp+=" "+new_cond[z]
			bool_val=str(eval(boolean_exp))
		else:
			bool_val="True"
		if bool_val==flag:
			final_dataset.append(row)
	#print(final_dataset)
	return final_dataset			

def generate_schema(tables) :
	new_schema = []
	for t in tables :
		new_schema += schema_of_tables[t]
	return new_schema

def compute_agg_func(agg_col ,agg_func ,table ,column_dict) :
	#print(table)
	if agg_func.lower() == "sum" :
		s = 0
		for row in table :
			s += row[column_dict[agg_col]]
		return s
	if agg_func.lower() == "average" :
		s = 0
		for row in table :
			s += row[column_dict[agg_col]]
		avg = float(s / len(table))
		return avg
	if agg_func.lower() == "max" :
		tmp = []
		for row in table :
			tmp.append(row[column_dict[agg_col]])
		rst = max(tmp)
		return rst
	if agg_func.lower() == "min" :
		tmp = []
		for row in table :
			tmp.append(row[column_dict[agg_col]])
		#print(tmp)
		rst = min(tmp)
		return rst
	if agg_func.lower() == "count" :
		return len(table)

def process_select_aggr(table ,aggregate_functions_map ,column_dict , groupby_flag) :
	final_table = []
	ans=[]
	for i in range(len(aggregate_functions_map)):
		( agg_col , agg_func ) = aggregate_functions_map[i] 
		func_res = compute_agg_func(agg_col , agg_func , table ,column_dict)
		ans.append(func_res)
	final_table.append(ans)
	if groupby_flag == False :
		for i in range(len(aggregate_functions_map)):
			(clnm , fncnm) = aggregate_functions_map[i]
			final_schema[fncnm.lower()] = i
	return final_table

def process_select(resulted_table ,columns ,column_dict):
	result = None
	if len(columns) == 1 :
		if columns[0] == "*" :
			result = resulted_table
		else :
			tmp_result=[]
			indx = column_dict[columns[0]]
			for row in resulted_table :
				temp=[]
				temp.append(int(row[indx]))
				tmp_result.append(temp)
			result = tmp_result
	elif len(columns) > 1 :
		tmp_result = []
		for row in resulted_table :
			temp = []
			for i in range(len(columns)) :
				temp.append(int(row[column_dict[columns[i]]]))
			tmp_result.append(temp)
		result = tmp_result
	if len(columns) == 1 and columns[0] == "*" :
		fs = column_dict
	else:
		fs={}
		for i in range(len(columns)) :
			fs[columns[i]]=i
	global final_schema
	final_schema = fs
	#print(final_schema)
	return result

def process_distinct(table):
	new_table = []
	if len(table) > 0 :
		prev = table[0]
		new_table.append(prev)
		for row in table :
			if row != prev :
				new_table.append(row)
				prev = row
			else :
				prev = row
	return new_table

def compute_group_by(table,groupby_flag,columns,groupby_coloumns,aggregate_functions_map,colname_dict) :
	if len(groupby_coloumns) > 1 :
		print("More than one columns are passed in the group by")
		sys.exit()
	if len(groupby_coloumns) == 0 :
		print("No condition present after group by")
		sys.exit()
	if len(columns) > 1 :
		print("more than one column with group by is not allowed")
		sys.exit()
	final_result = None 
	if len(groupby_coloumns)==1 :
		grpby_col_name = groupby_coloumns[0]
		#print(colname_dict, grpby_col_name)#######
		indx = colname_dict[grpby_col_name]
		#print(indx)
		if len(columns) == 1 :
			if grpby_col_name not in columns :
				print("group by column must be there in select")
				sys.exit()
			column = columns[0]
			if grpby_col_name != column :
				print("something is not right!")
				sys.exit()
			tmp_result   = sorted(table, key=lambda x: x[indx])
			if len(aggregate_functions_map) == 0 :
				#print(tmp_result)
				#print(columns)
				tmp2_result  = process_select(tmp_result ,columns ,colname_dict)
				#print(tmp2_result)
				final_result = process_distinct(tmp2_result)
				#final_result = tmp2_result
			else :
				tmp2_result = []
				table_in_making = []
				if len(tmp_result) > 0 :
					prev = tmp_result[0][indx]
					#print("prev",prev)
					for row in tmp_result :
						#print("row" , row)
						if row[indx] == prev :
							tmp2_result.append(row)
						else :
							#print("inside else ->")
							tbl = process_select_aggr(tmp2_result ,aggregate_functions_map ,colname_dict ,groupby_flag )
							#print("tbl",tbl)
							tmp_list = [prev] + tbl[0]
							#print("tmp_list",tmp_list)
							table_in_making.append(tmp_list)
							tmp2_result.clear()
							tmp2_result.append(row)
							prev = row[indx]
					tbl = process_select_aggr(tmp2_result, aggregate_functions_map,colname_dict,groupby_flag)
					tmp_list = [prev] + tbl[0]
					table_in_making.append(tmp_list)
				final_result = table_in_making
				final_schema[grpby_col_name] = 0
				for i in range(len(aggregate_functions_map)):
					(colnm , fncnm) = aggregate_functions_map[i]
					final_schema[fncnm.lower()] = i+1
		elif len(columns) == 0 :
			if len(aggregate_functions_map) == 0 :
				print("Something is not right!")
				sys.exit()
			else :
				colms = []
				for mp in aggregate_functions_map :
					(colnam , fnc_name) = mp
					colms.append(colnam)
				if grpby_col_name not in colms :
					print("Group by column must be there in Select ")
					sys.exit()
				tmp_result   = sorted(table, key=lambda x: x[indx])
				tmp2_result = []
				table_in_making = []
				if len(tmp_result) > 0 :
					prev = tmp_result[0][indx]
					for row in tmp_result :
						if row[indx] == prev :
							tmp2_result.append(row)
						else :
							tbl = process_select_aggr ( tmp2_result ,  aggregate_functions_map , colname_dict , groupby_flag)
							table_in_making.append(tbl[0])
							tmp2_result.clear()
							tmp2_result.append(row)
							prev = row[indx]
					tbl = process_select_aggr(tmp2_result, aggregate_functions_map, colname_dict, groupby_flag)
					table_in_making.append(tbl[0])
				final_result = table_in_making
				for i in range(len(aggregate_functions_map)):
					(colnm , fncnm) = aggregate_functions_map[i]
					final_schema[fncnm.lower()] = i
	else :
		print("query invalid")
		sys.exit() 
	return final_result	

def compute_orderby(table , orderby_column , asc_desc_flag , column_dict , asc_desc , columns , dstnct_flag) :
	if orderby_column not in columns :
		print("order by column is not there in the select")
		sys.exit()
	indx = column_dict[orderby_column]
	tmp_result = None
	if asc_desc_flag == False or ( asc_desc_flag == True and asc_desc.lower() == "asc" ) :
		tmp_result = sorted(table, key=lambda x: x[indx])
	else:
		tmp_result = sorted(table, key=lambda x: x[indx] , reverse = True)
	if dstnct_flag == True :
		tmp_result = process_distinct(tmp_result)
	return tmp_result


def process_query(query):
	columns=[] # name of columns of tables which appear in query after select
	groupby_coloumns=[] # columns of tables which appear in query after group by
	orderby_column = []
	asc_desc = None
	tables=[] # name of tables which appear in query
	conditionals=[] # contains expressions which involves some conditional operations
	aggregate_functions_map=[]
	components = []
	parsed_query = sqlparse.parse(query)[0].tokens
	#print(parsed_query)              
	command = sqlparse.sql.Statement(parsed_query).get_type()  				# select
	cmpnnts = sqlparse.sql.IdentifierList(parsed_query).get_identifiers()
	if command.strip().lower()!="select":
		print("Query not supported")
		sys.exit()
	condition    = None
	distinct     = False	
	from_flag    = False
	select_flag  = False
	where_flag   = False
	groupby_flag = False
	orderby_flag = False
	asc_desc_flag= False
	no_of_select = 0
	no_of_from   = 0
	for c in cmpnnts :
		components.append(str(c))
	#print(components)
	for c in components :
		c=c.strip()
		if c.lower() == "select" :
			if no_of_select > 1 :
				print("Mulitple select used")
				sys.exit()
			select_flag = True
			no_of_select += 1
			#print("read select") ######3
			continue
		if c.lower() == "distinct" :
			if distinct == True :
				print("Mulitple distinct used")
				sys.exit()
			distinct = True
			#print("read distinct")######
			continue
		if c.lower() == "from" :
			if no_of_from > 1:
				print("Mulitple from used")
				sys.exit()
			from_flag = True
			no_of_from += 1
			#print("read from") ######3
			continue
		if select_flag == True and no_of_from == 0 :
			columns = c.split(",")
			#print("read columns:" , columns) ######3
			select_flag = False
			continue
		if no_of_select == 1 and from_flag == True :
			tables = (c.replace(" ","")).strip().split(",")
			#print("read tables:" , tables) ######333
			from_flag = False
			continue
		if c.lower().startswith("where") and no_of_from == 1 and groupby_flag == False:
			if where_flag == True:
				print("Mulitple where used")
				sys.exit()
			where_flag = True
			#print("read where")####
			condition = c[6:].strip()
			#print("read condition:" , condition)#####
			#print(condition)
			continue
		if c.lower() == "group by" :
			if groupby_flag == True :
				print("Mulitple group by used")
				sys.exit()
			groupby_flag = True	
			continue
		if c.lower() == "order by" :
			if orderby_flag == True:
				print("Mulitple group by used")
				sys.exit()
			orderby_flag = True
			continue
		if groupby_flag == True and orderby_flag == False :
			groupby_coloumns = c.split(",")
			#print(groupby_coloumns)
			continue
		if orderby_flag == True and asc_desc_flag == False :
			orderby_column = c.strip()
			tlst = orderby_column.split(" ")
			orderby_column = tlst[0].strip()
			#print(orderby_column)
			#print("orderby_column" , orderby_column)
			if len(tlst) > 1 and(tlst[1].lower() == "asc" or tlst[1].lower() == "desc") :
				asc_desc = tlst[1].strip()
				asc_desc_flag = True
			continue
		else :
			print("Query Invalid")
			sys.exit()
	columns,aggregate_functions_map = get_col_aggr(columns, groupby_flag)

	joined_table = join(tables) 
	new_schema = generate_schema(tables)
	
	column_dict={}
	for i in range(len(new_schema)):
		column_dict[new_schema[i]]=i

	orderby_computed_table = None
	distinct_computed_Table= None
	groupby_computed_table = None
	where_computed_table   = None
	final_table            = None
	
	if where_flag == True :
		where_computed_table = compute_conditions(condition,joined_table,column_dict)
	else :
		where_computed_table = joined_table
	
	if groupby_flag == False :
		if len(aggregate_functions_map) == 0 :
			groupby_computed_table = process_select(where_computed_table , columns , column_dict )
		elif len(columns) == 0 :
			groupby_computed_table = process_select_aggr(where_computed_table ,aggregate_functions_map ,column_dict, groupby_flag)
		else :
			print("Both columns and Aggregate functions are not allowed in SELECT without group by clause")
			sys.exit()	
	else :
		groupby_computed_table = compute_group_by(where_computed_table , groupby_flag , columns,   \
			   						              groupby_coloumns ,aggregate_functions_map,column_dict)
	
	if distinct == True and groupby_flag == False :
		distinct_computed_Table = process_distinct(groupby_computed_table)
	else :
		distinct_computed_Table = groupby_computed_table
	#print(distinct_computed_Table)

	if orderby_flag == True :
		orderby_computed_table = compute_orderby(distinct_computed_Table , orderby_column ,\
												 asc_desc_flag , final_schema , asc_desc , columns , distinct) 
	else :
		orderby_computed_table = distinct_computed_Table

	display(orderby_computed_table , aggregate_functions_map)

if __name__ == "__main__":
	if len(sys.argv)==2:
		query = sys.argv[1]
		if query[-1] != ";" :
			print("Semi colon missing")
			sys.exit()
		else :
			query = query[:-1]
		#query = query.lower()
		metadatatxt()	
		process_query(query)

	else:
		print("Invalid no of Arguments")
		sys.exit()
