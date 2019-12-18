find * | grep ".txt" | grep -v lexicon | xargs -r -I @ cat @ | grep -v "no" | grep -v "\[.*\]" | sed 's/<.*>//g'

