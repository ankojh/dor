# Geographic ambiguity lexicons

`common-english.txt` is the intersection of the 50,000 highest-frequency
English tokens in Hermit Dave's FrequencyWords, OpenSubtitles 2018 list, with
lowercase words in Webster's Second International (`web2`). This produces
21,954 terms and excludes capitalized place names such as Delhi. The frequency
list is available at https://github.com/hermitdave/FrequencyWords/blob/525f9b560de45753a5ea01069454e72e9aa541c6/content/2018/en/en_50k.txt ; the underlying OpenSubtitles data is licensed CC BY-SA 4.0
(https://creativecommons.org/licenses/by-sa/4.0/). Webster's 1934 copyright
has lapsed according to the FreeBSD `/usr/share/dict/README` distributed with
`web2`. `proper-words.txt` is the intersection of those same frequency tokens
with capitalized Webster entries; it is used only for smaller non-capital places.
These files contain tokens only and retain their source order.

`given-names.txt` is the union of the first 1,500 entries in each of the US
Census Bureau's 1990 frequency-ranked male and female given-name tables:
https://www2.census.gov/topics/genealogy/1990surnames/dist.male.first and
https://www2.census.gov/topics/genealogy/1990surnames/dist.female.first .
US federal government data is public domain. These lists are heuristic negative
evidence, not exhaustive lists of English words or world names.
