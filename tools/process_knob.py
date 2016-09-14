f = open('mysql_knob.txt','r+')
data = f.readlines()
f.close()

f = open('mysql_knobs.json','w')


f.write("[\n")

l = 0
for x in data:
    x = x.replace("\n","")
    l += 1
    f.write("  {\n    \"model\":\"website.KNOB_PARAMS\",\n    \"fields\":{\n")
    f.write("      \""+"db_type"+ "\":\"" +"MYSQL" + "\",\n")
    f.write("      \"params" +  "\":\"" + str(x) + "\"\n")
    if l == len(data):
        f.write("    }\n  }\n")
    else:
        f.write("    }\n  },\n")
f.write("]")

f.close()
