## Tweets to Postgresql with airflow and Spark

**Intro:**
  I developed a system that get tweets in Twitter API and transform it on data lake to push them on database with Postgresql.

**Motivation and Goal:**
  I was tryin understand how Airflow works to orchestrate tasks and how Spark manipulate data, basicaly i was studyin a real case of ETL and Datalake with this two tools.
  
**Phases:**
  - Testing Twitter API
  - Connecting to Twiiter
  - Creating a DAG
  - Getting the data
  - First tranformation of the data (Bronze to Silver)
  - Second transformation of the data (Silver to Gold)
  - Putting the transformations in the Airflow dag as task
  - Filling in a table database with the tweets of the Gold stage with a task in Airflow.


## Welcome to GitHub Pages

You can use the [editor on GitHub](https://github.com/afnscbrl/TwitterToSql/edit/main/README.md) to maintain and preview the content for your website in Markdown files.

Whenever you commit to this repository, GitHub Pages will run [Jekyll](https://jekyllrb.com/) to rebuild the pages in your site, from the content in your Markdown files.

### Markdown

Markdown is a lightweight and easy-to-use syntax for styling your writing. It includes conventions for

```markdown
Syntax highlighted code block

# Header 1
## Header 2
### Header 3

- Bulleted
- List

1. Numbered
2. List

**Bold** and _Italic_ and `Code` text

[Link](url) and ![Image](src)
```

For more details see [Basic writing and formatting syntax](https://docs.github.com/en/github/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax).

### Jekyll Themes

Your Pages site will use the layout and styles from the Jekyll theme you have selected in your [repository settings](https://github.com/afnscbrl/TwitterToSql/settings/pages). The name of this theme is saved in the Jekyll `_config.yml` configuration file.

### Support or Contact

Having trouble with Pages? Check out our [documentation](https://docs.github.com/categories/github-pages-basics/) or [contact support](https://support.github.com/contact) and we’ll help you sort it out.
