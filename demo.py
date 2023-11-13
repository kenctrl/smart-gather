#editor.detectIndentation#
import openai
OPENAI_API_KEY = "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"

def main():
    client = openai.OpenAI(api_key = OPENAI_API_KEY)
    gpt_input = input('Enter schema...\n')
    response = client.chat.completions.create(
      model="gpt-3.5-turbo",
      messages=[
          {"role": "system",
           "content": "Given this database schema:\n" + gpt_input + "\n\nGenerate an on-topic phrase for the database that is less than 6 words."}
      ],
      temperature=0,
      max_tokens=256
    )
    print(response.choices[0].message.content)

if __name__ == '__main__':
    main()