import pandas as pd
import json
import re
import unicodedata
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
import nltk
# Add this line below your other nltk.download calls:
nltk.download('punkt_tab')

# Download necessary NLTK data
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

def clean_text(text):
    if not isinstance(text, str): return ""
    # 1. Unicode normalization & Lowercasing
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8').lower()
    # 2. Remove HTML & URLs
    text = re.sub(r'<.*?>|http\S+', '', text)
    # 3. Remove punctuation
    text = re.sub(r'[^\w\s]', '', text)
    # 4. Tokenization
    tokens = word_tokenize(text)
    # 5. Stopword removal, Lemmatization, Numeric-only & Length filters
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()
    
    clean_tokens = [
        lemmatizer.lemmatize(t) for t in tokens 
        if t not in stop_words and not t.isdigit() and len(t) >= 2
    ]
    return " ".join(clean_tokens), clean_tokens

# Load your 500-item raw data
with open('trend_intelligence_pipeline/data/raw/products_raw_500.json', 'r') as f:
    data = json.load(f)

df = pd.DataFrame(data)

# Combine fields for cleaning
df['text_raw'] = df['product_name'] + " " + df['tagline']
df['text_clean'], df['tokens'] = zip(*df['text_raw'].apply(clean_text))
df['token_count'] = df['tokens'].apply(len)

# Save to the required path
df[['text_raw', 'text_clean', 'tokens', 'token_count']].to_csv('trend_intelligence_pipeline/data/processed/products_clean.csv', index=False)
print("Preprocessing complete!")