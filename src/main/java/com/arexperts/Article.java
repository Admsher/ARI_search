package com.arexperts;


/**
 * Class to represent an article.
 */
public class Article {
    /**
     * The text of the article
     */
    String articleText;

    /**
     * The ID of the article
     */
    String articleID;

    /**
     * Static method to construct an Article
     * @param text the text of the article
     * @param id the ID of the article
     * @return a new Article
     */
    public static Article build(String text, String id) {
        Article newArticle =  new Article();

        newArticle.articleID = id;
        newArticle.articleText = text;

        return newArticle;
    }
}
