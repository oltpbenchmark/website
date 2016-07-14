import MySQLdb
conn = MySQLdb.connect(host="localhost",user="zbh",passwd="zbh",db="benchdb")
cursor = conn.cursor()
sql = "truncate table website_featured_params" 
cursor.execute(sql)
sql = "truncate table website_learning_params"
cursor.execute(sql) 
 
