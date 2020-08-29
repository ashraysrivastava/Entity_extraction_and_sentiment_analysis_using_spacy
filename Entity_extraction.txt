import pymongo
import spacy
from textblob import TextBlob
import nltk
import en_core_web_sm
nlp = en_core_web_sm.load()
from spacy.lang.en import English
from html.parser import HTMLParser
parser = English()
nltk.download('stopwords')#to download stopwords
noise_list = ['.',',','?',':',';','"',"(",')','/']
              
              
def sentiments(new):#function for sentiment analysis and score
    sentiments = TextBlob(new)
    return("Sentiment polarity Score : ", sentiments.sentiment.polarity, "Sentiment Score subjectivity: ", sentiments.sentiment.subjectivity)
   


def entity_extraction(text):#function for entity extraction
    doc=nlp(text)
    ls=[]
    x=''
    y=''
    for entity in doc.ents:
        if entity.text != '\n':
            x = "".join(entity.text)
            y="".join(spacy.explain(entity.label_))
        
    return (x+", "+y)

def part_of_speech_tagging(pos):#function for part of speech tagging
    doc=nlp(pos)
    x=''
    y=''
    for token in doc:
        x="".join(token.text)
        y="".join(spacy.explain(token.pos_))
    return (x+" "+y)


class MLStripper(HTMLParser):#class to remove html noise from text 
    def __init__(self):
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):#function to remove html tags
    s = MLStripper()
    s.feed(html)
    return s.get_data()

def tokenize(text):#function to tokenize the data
    
    lda_tokens = []
    tokens = parser(text)
    for token in tokens:
        if token.orth_.isspace():
            continue
        elif token.like_url:
            lda_tokens.append('URL')
        elif token.orth_.startswith('@'):
            lda_tokens.append('SCREEN_NAME')
        else:
            lda_tokens.append(token.text)
    return lda_tokens

def noise_removal(text):#here we have implemnted our noise removal by calling all the functions
    text =strip_tags(text)
    tokens = tokenize(text)
    tokens = [token for token in tokens if token not in noise_list]
    noise_free_text = " ".join(tokens)
    #print(noise_free_text)
    return noise_free_text
    
myclient = pymongo.MongoClient("mongodb://localhost:27017/")#connecting to mongodb
db = myclient["Pilotbot_Dev"]
mycol = db["NeevaFaq"]#creating a collection
def searching_collection():   
    for y in mycol.find():#here we are iterating the records 
        #print(type(y['title']))
        #print(y)
        id=y["_id"]
        data = y['title']#field for which entity extraction is to be done
        data1=y['acceptedAnswer']#here we specify the field for which the entity extraction is to be done
        #print(token(data))
        #print(data)
        temp=noise_removal(data)
        entity=str(entity_extraction(temp))#here we have done entity extraction for our title
        #print(entity+ "  "+temp)
        pos=str(part_of_speech_tagging(temp))
        #print(pos)
        score=sentiments(data)
        temp1=noise_removal(data1)
        #print(temp1)
        entity1=str(entity_extraction(temp1))#here we have done entity extraction for our acceptedAnswer
        #print(entity1+ "  "+temp1)
        pos1=str(part_of_speech_tagging(temp1))
        #print(pos)
        score1=sentiments(data1)
        mycol.update({"_id": id }, {"$set": {"search_field_for_Accepted_answer":temp1,"entity_for_accepted_answers":entity1,"part_of_speech_tagging_for_accepted_answers":pos1,"sentiment_and_score_for_accepted_Answers":score1,"search_field_for_title":temp,"entity_for_title": entity,"part_of-speech_tagging_for_title":pos,"sentiment_and_score_for_title":score}})
        ##mycol.insert_one({"sentiment_and_score_for_title":score,"sentiment_and_score_for_accepted_Answers":score1,"search_field_for_Accepted_answer":data1,"entity_for_accepted_answers":entity1,"part_of_speech_tagging_for_accepted_answers":pos1,"search_field_for_title":data,"entity_for_title": entity,"part_of-speech_tagging_for_title":pos})#here we have pushed our new records into the new collection of  mongoDB
      
searching_collection()#here we are calling the function
 
# for x in mycol1.find():#we have printed our new records here
#     print(x)






    
