import pandas as pd
import json
import numpy as np
from collections import Counter
from nltk import ngrams

# 1. Load the cleaned data
df = pd.read_csv('trend_intelligence_pipeline/data/processed/products_clean.csv')
# Convert string representation of list back to actual list
df['tokens'] = df['tokens'].apply(eval) 

# 2. Vocabulary Extraction
all_tokens = [token for sublist in df['tokens'] for token in sublist]
vocab = sorted(list(set(all_tokens)))
vocab_dict = {word: i for i, word in enumerate(vocab)}

# 3. Bag-of-Words (BoW) Matrix - Manual Implementation
bow_matrix = np.zeros((len(df), len(vocab)), dtype=int)
for i, tokens in enumerate(df['tokens']):
    counts = Counter(tokens)
    for word, count in counts.items():
        if word in vocab_dict:
            bow_matrix[i, vocab_dict[word]] = count

# 4. N-gram Frequency Distribution
unigrams = Counter(all_tokens)
bigrams = Counter([bg for tokens in df['tokens'] for bg in ngrams(tokens, 2)])

# 5. One-Hot Encoding (for first 5 documents)
# One-hot is similar to BoW but binary (1 if present, 0 if not)
one_hot_subset = (bow_matrix[:5] > 0).astype(int)

# --- Save Results ---
import os
os.makedirs('trend_intelligence_pipeline/data/features', exist_ok=True)

with open('trend_intelligence_pipeline/data/features/vocab.json', 'w') as f:
    json.dump(vocab_dict, f)

np.save('trend_intelligence_pipeline/data/features/bow_matrix.npy', bow_matrix)
np.save('trend_intelligence_pipeline/data/features/one_hot_subset.npy', one_hot_subset)

print(f"Feature Engineering Complete. Vocab size: {len(vocab)}")