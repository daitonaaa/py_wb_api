import spacy
from transformers import GPT2LMHeadModel, GPT2Tokenizer

if __name__ == '__main__':
    # Загрузка модели языка
    nlp = spacy.load("ru_core_news_sm")

    text1 = "Российский педиатр Сергей Бутрий, известный как автор блога «Заметки детского врача», покинул Россию"
    text2 = "Автора блога «Записки детского врача» в феврале арестовали на 10 суток по делу о возбуждении ненависти и вражды"

    doc1 = nlp(text1)
    doc2 = nlp(text2)


    unique_words = set([token.text.lower() for token in doc1 if token.is_alpha])
    unique_words.update([token.text.lower() for token in doc2 if token.is_alpha])

    new_text = " ".join(unique_words)
    print(new_text)


    model = GPT2LMHeadModel.from_pretrained("gpt2")
    tokenizer = GPT2Tokenizer.from_pretrained("gpt2")

    # Подготовка данных для модели
    input_ids = tokenizer.encode(new_text, return_tensors="pt")

    # Генерация нового текста на основе уникальных слов из исходных текстов
    output = model.generate(input_ids, max_length=500, num_return_sequences=1, no_repeat_ngram_size=2, top_k=50)

    generated_text = tokenizer.decode(output[0], skip_special_tokens=True)
    print('gen')
    print(generated_text)

