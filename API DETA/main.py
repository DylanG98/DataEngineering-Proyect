from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse
from deta import Deta, Drive
import pandas as pd
from io import BufferedIOBase

#######################################################################################
#######################################################################################

app = FastAPI()                 #####   Run my API   

#######################################################################################
#######################################################################################

deta = Deta("e0nd0nf9_5NuXHPhKwCkyqtnhhRVtvBVXLhqwMeMq")  
drive = deta.Drive("myfiles")    #####  Create and execute my drive file on Deta

#######################################################################################
#######################################################################################

##### I used this class for the proper functioning of my "outputs"
##### because I used dictionaries and not JSON for the result of my queries.


class DriveStreamingBody:
    def __init__(self, res: BufferedIOBase):
        self.__stream = res

    @property
    def closed(self):
        return self.__stream.closed

    def read(self, size: int = None):
        return self.__stream.read(size)

    def iter_chunks(self, chunk_size: int = 1024):
        while True:
            chunk = self.__stream.read(chunk_size)
            if not chunk:
                break
            yield chunk
        
    def iter_lines(self, chunk_size: int = 1024):
        while True:
            chunk = self.__stream.readline(chunk_size)
            if not chunk:
                break
            yield chunk

    def close(self):
        # close stream
        try:
            self.__stream.close()
        except:
            pass



#######################################################################################
#######################################################################################


@app.get("/")
def read_root():
    return {"Bienvenidos a mi API": "Dylan Guzman"}


##### These two functions were used for loading CSV files, 
##### first their rendering and then the download.  (Deta Docs)


@app.get("/render", response_class=HTMLResponse)
def render():
    return '''
    <form action="/upload" enctype="multipart/form-data" method="post">
        <input name="file" type="file">
        <input type="submit">
    </form>'''

@app.post("/upload")
def upload(file: UploadFile = File(...)):
    name = file.filename
    f = file.file
    res = drive.put(name, f)
    return res

#######################################################################################
#######################################################################################

##### QUERIES

@app.get("/get_word_count/{plataforma}/{keyword}")    
def get_word_count(plataforma:str,keyword:str):
    res = drive.get(f"{plataforma}.csv")
    df = pd.read_csv(res)
    count=0
    for index, i in enumerate(df['title']):
        if keyword in i:
            count+=1
    dicc={  'plataforma':plataforma,
            'keyword': keyword,
            'cantidad': count}
    
    return DriveStreamingBody(dicc)


@app.get("/get_score_count/{plataforma}/{score}/{anio}")    
def get_score_count(plataforma:str,score:int,anio:int):
    res = drive.get(f"{plataforma}.csv")
    df = pd.read_csv(res)
    type = 'movie'
    count=0
    for index,i in enumerate(df['title']):
        if df['release_year'][index]== anio:
            if df['score'][index]>score:
                if df['type'][index] in type:
                    count+=1

    dicc = {'plataforma' : plataforma,
            'score'      : score,
            'año'        : anio,
            'cantidad'   : count}

    
    return DriveStreamingBody(dicc)




@app.get("/get_second_score/{plataforma}")   
def Get_second_score(plataforma:str):
    res = drive.get(f"{plataforma}.csv")
    df = pd.read_csv(res)
    values=[]
    type='movie'

    for index, i in enumerate(df['title']):
        if df['score'][index]==100:
            if df['type'][index]==type:
                values.append(df['title'][index])

    values.sort()
    result = values[1]
    
    dicc = {'plataforma': plataforma,
            'titulo': result}
            
    
    return DriveStreamingBody(dicc)



@app.get("/get_longest/{plataforma}/{duration_type}/{release_year}")   
def Get_second_score(plataforma:str,duration_type:str,release_year:int):
    res = drive.get(f"{plataforma}.csv")
    df = pd.read_csv(res)
   
    type='movie'
    result=0

    for index, i in enumerate(df['title']):
        if df['duration_type'][index] == duration_type:
            if df['release_year'][index]==release_year:
                if df['type'][index]in type:
                    if df['duration_int'][index]>result:
                        result = df['duration_int'][index]
                        title = df['title'][index]
    
    dicc = {'titulo': title,
            'duracion': result}
            
    
    return DriveStreamingBody(dicc)


@app.get("/get_rating_count/{rating}")
def get_rating_count(rating:str):
    a = drive.get("amazon.csv")
    h = drive.get("hulu.csv")
    n = drive.get("netflix.csv")
    d = drive.get("disney.csv")
    dfa= pd.read_csv(a)
    dfh= pd.read_csv(h)
    dfd= pd.read_csv(d)
    dfn= pd.read_csv(n)

    count = 0

    for index, i in enumerate (dfa['rating']):
        if dfa['rating'][index] == rating:
            count+=1
    for index, i in enumerate (dfn['rating']):
        if dfn['rating'][index] == rating:
            count+=1
    for index, i in enumerate (dfh['rating']):
        if dfh['rating'][index] == rating:
            count+=1
    for index, i in enumerate (dfd['rating']):
        if dfd['rating'][index] == rating:
            count+=1

    dicc = {'rating': rating,
            'cantidad':count}
            
    return DriveStreamingBody(dicc)

