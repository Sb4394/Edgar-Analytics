from datetime import datetime
import sys
ip_location=-1
date_location=-1
time_location=-1
cik_location=-1
accession_location=-1
extension_location=-1
separator=','

def infer_header(header,separator):
    """
    Takes inputs: header, separator
    Captures the required columns' information, i.e., their indexes.
    """
    global ip_location,date_location,time_location,cik_location,accession_location,extension_location
    columns = header.split(separator)
    for column in columns:
        if ('IP' in column.upper()):
            ip_location = columns.index(column)
        if ('DATE' in column.upper()):
            date_location = columns.index(column)
        if ('TIME' in column.upper()):
            time_location = columns.index(column)
        if ('CIK' in column.upper()):
            cik_location = columns.index(column)
        if ('ACCESSION' in column.upper()):
            accession_location = columns.index(column)
        if ('EXTENSION' in column.upper()):
            extension_location = columns.index(column)

def reader(filename,session_file,output_file,required_columns):
    """for reading the requried lists of data"""

    separator=','
    with open(session_file, 'r+') as file:
        session=float(file.readline()) #get session timeout period

    with open(filename, 'r+') as file:
        
        #infer_header
        infer_header(file.readline(),separator)

        output={} #dictionary for logging
        files=1
        sec=1
        ip_list=[] #list of ip addresses that we read

        for line in file:
            line = line.strip()   #we don't want '\n' to appear, so we use strip()
            columns = line.split(separator)

            # need only ip,date,time
            ip=columns[required_columns[0]()]
            date=columns[required_columns[1]()]
            timer=columns[required_columns[2]()]
            cik=columns[required_columns[3]()]
            accession=columns[required_columns[4]()]
            extension=columns[required_columns[5]()]

            #combine date and time
            time=(datetime.strptime(date+":"+timer, '%Y-%m-%d:%H:%M:%S'))

            # for each ip in ip_list, check if the session time is exceeded
            #if yes, call writer function, remove ip from the list and also from the log dictionary
            for l in ip_list:
                if(time-output[l][2]).total_seconds()>session:
                    #writer function
                    writer(output[l],output_file)
                    ip_list.remove(l)
                    output.pop(l)
                
            # if new ip, add it in the log dictionary
            if ip not in output:
                ip_list.append(ip)
                end_time=time  # for the first entry start and end time are the same
                output[ip]=[ip,time,end_time,sec,files]

            # if already seen, update the end time, session duration and file count
            else:
                output[ip][2]=time
                output[ip][3]=int((time- output[ip][1]).total_seconds())+1
                output[ip][4]+=1

        # write all log which didn't exceed the session duration, but since end of file implies, it should identify all sessions regardless of the period of inactivity       
        for l in ip_list:
            writer(output[l],output_file)
    
                
def writer(values,output_file):
    """for writing the values into the output_file"""
    file=open(output_file,'a') #opens file in append mode.  The pointer is placed at the end of the file. A new file is created if one with the same name doesn't exist
    
    #writes ip, start time, end time, session duration(in sec), webpage count
    file.write(values[0]+","+str(values[1])+","+str(values[2])+","+str(values[3])+","+str(values[4])+"\n")
    
    file.close()

if __name__ == '__main__':
    # Assign input, session and output files.
    input_file=sys.argv[1]
    session_file = sys.argv[2]
    output_file= sys.argv[3]

    #we only need few columns from the input file
    required_columns=[lambda:ip_location,lambda:date_location,lambda:time_location,lambda:cik_location,lambda:accession_location,lambda:extension_location]  
   
    #reads the file line by line and calls the writer function
    reader(input_file,session_file,output_file,required_columns) 


        

    
   
