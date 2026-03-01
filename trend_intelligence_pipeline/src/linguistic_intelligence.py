import pandas as pd
import json
import numpy as np
import os
from collections import Counter
from nltk import ngrams, edit_distance
import math

# 1. Load Data
df = pd.read_csv('trend_intelligence_pipeline/data/processed/products_clean.csv')
df['tokens'] = df['tokens'].apply(eval)
with open('trend_intelligence_pipeline/data/features/vocab.json', 'r') as f:
    vocab = json.load(f)

# 2. Stats & Frequencies
all_tokens = [t for tokens in df['tokens'] for t in tokens]
unigrams = Counter(all_tokens)
bigrams = Counter([bg for tokens in df['tokens'] for bg in ngrams(tokens, 2)])
vocab_size = len(vocab)
avg_len = df['token_count'].mean()

# 3. Minimum Edit Distance (Duplicate Detection)
# We check first 50 titles to save time (Threshold = 3)
duplicates = []
titles = df['text_raw'].head(50).tolist()
for i in range(len(titles)):
    for j in range(i + 1, len(titles)):
        if edit_distance(titles[i], titles[j]) < 3:
            duplicates.append((titles[i], titles[j]))

# 4. Unigram Language Model (Probabilities)
total_count = len(all_tokens)
unigram_probs = {word: count / total_count for word, count in unigrams.items()}

# 5. Perplexity (Held-out 5 descriptions)
def calculate_perplexity(description_tokens, model_probs):
    entropy = 0
    for t in description_tokens:
        prob = model_probs.get(t, 1 / (total_count + vocab_size)) # Laplace smoothing
        entropy += -math.log2(prob)
    avg_entropy = entropy / len(description_tokens) if len(description_tokens) > 0 else 0
    return 2 ** avg_entropy

held_out = df['tokens'].tail(5)
perplexities = [calculate_perplexity(tokens, unigram_probs) for tokens in held_out]

# --- Generate Report ---
os.makedirs('reports', exist_ok=True)
with open('reports/trend_summary.txt', 'w') as f:
    f.write(f"--- TrendScope Linguistic Report ---\n")
    f.write(f"Vocabulary Size: {vocab_size}\n")
    f.write(f"Avg Description Length: {avg_len:.2f} tokens\n\n")
    f.write(f"Top 30 Unigrams: {unigrams.most_common(30)}\n\n")
    f.write(f"Top 20 Bigrams: {bigrams.most_common(20)}\n\n")
    f.write(f"Potential Duplicates (Edit Distance < 3): {len(duplicates)}\n")
    f.write(f"Avg Perplexity (Held-out): {np.mean(perplexities):.2f}\n")

print("Stage 5: Report Generated at reports/trend_summary.txt")