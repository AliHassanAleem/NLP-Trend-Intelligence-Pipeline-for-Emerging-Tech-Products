import pandas as pd
import json
import numpy as np
from collections import Counter
from Levenshtein import distance
import os
import math

def find_duplicates(names, threshold=3):
    duplicates = []
    for i in range(len(names)):
        for j in range(i+1, len(names)):
            if distance(names[i], names[j]) <= threshold:
                duplicates.append((names[i], names[j]))
    return duplicates

def unigram_probs(unigrams, total_tokens):
    return {word: count / total_tokens for word, count in unigrams.items()}

def perplexity(text_tokens, probs, vocab_size):
    log_prob = 0
    n = len(text_tokens)
    for token in text_tokens:
        p = probs.get(token, 1 / vocab_size)  # Laplace smoothing
        log_prob += math.log(p)
    return math.pow(2, -log_prob / n) if n > 0 else 0

if __name__ == "__main__":
    df = pd.read_csv("data/processed/products_clean.csv")
    with open("data/features/unigram_freq.json", "r") as f:
        unigrams = json.load(f)
    with open("data/features/bigram_freq.json", "r") as f:
        bigrams = {eval(k): v for k, v in json.load(f).items()}  # Str to tuple
    with open("data/features/vocab.json", "r") as f:
        vocab = json.load(f)
    
    total_tokens = sum(df['token_count'])
    probs = unigram_probs(unigrams, total_tokens)
    
    # Held-out: Last 5
    held_out = df['tokens'].tail(5).apply(eval)
    perplexities = [perplexity(tokens, probs, len(vocab)) for tokens in held_out]
    
    tags_counter = Counter(chain(*df['tags'].apply(eval)))
    
    duplicates = find_duplicates(df['product_name'].tolist())
    
    avg_length = df['token_count'].mean()
    
    os.makedirs("reports", exist_ok=True)
    with open("reports/trend_summary.txt", "w") as f:
        f.write("Top 30 Unigrams:\n")
        f.write(str(Counter(unigrams).most_common(30)) + "\n\n")
        f.write("Top 20 Bigrams:\n")
        f.write(str(Counter(bigrams).most_common(20)) + "\n\n")
        f.write("Most Common Tags:\n")
        f.write(str(tags_counter.most_common(10)) + "\n\n")
        f.write(f"Vocabulary Size: {len(vocab)}\n")
        f.write(f"Average Description Length: {avg_length:.2f}\n")
        f.write("Duplicates (Edit Distance <=3):\n")
        f.write(str(duplicates) + "\n\n")
        f.write("Unigram Probabilities (sample):\n")
        f.write(str(dict(list(probs.items())[:10])) + "\n\n")
        f.write("Perplexity for 5 Held-Out:\n")
        f.write(str(perplexities) + "\n")
    
    print("Report saved to reports/trend_summary.txt")