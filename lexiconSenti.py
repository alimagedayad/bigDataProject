import nltk
import re
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

def expandNOTs(review):
    print(f'review: {review}')
    rev = []
    # for word in review.split(" "):
    for word in review:
        if re.search("n't", word):
            rev.append(word.split("n't")[0])
            rev.append("not")
        else:
            rev.append(word)
    return ''.join(rev)

def format_output(output_dict):
    polarity = "neutral"

    if (output_dict['compound'] >= 0.05):
        polarity = "positive"

    elif (output_dict['compound'] <= -0.05):
        polarity = "negative"

    return polarity

def predict_sentiment(text):
    print('pS: ', text)
    output_dict = analyzer.polarity_scores(text)
    return format_output(output_dict)


df = pd.read_csv('data/dataset.csv')

# review_title,review_text

# Drop NAs
df = df.dropna()

# Clean the data
df['review_title'] = df['review_title'].str.replace(
    "\d+ out of \d+ found this helpful. Was this review helpful\\? Sign in to vote. Permalink", " ", regex=True)
df['review_text'] = df['review_text'].str.replace(
    "\d+ out of \d+ found this helpful. Was this review helpful\\? Sign in to vote. Permalink", " ", regex=True)

df["vader_prediction_rtitle"] = df["review_title"].apply(lambda x: expandNOTs(x)).apply(lambda x: predict_sentiment(str(x)))
df["vader_prediction_rtext"] = df["review_text"].apply(lambda x: expandNOTs(x)).apply(lambda x: predict_sentiment(str(x)))

#     .apply(predict_sentiment)
# df["vader_prediction_rtext"] = df["review_text"].str.encode('utf-8').apply(predict_sentiment)

df.to_csv('sentiment_vader.test.csv', index=False)