package com.webcrawler.processing;

import java.util.Locale;
import java.util.regex.Pattern;

public class TermSearcher {

    private static final String WORD_CHAR_CLASS = "[\\p{L}\\p{N}]";

    public boolean search(String text, String term, boolean wholeWord) {
        if (text == null || term == null || term.isBlank()) {
            return false;
        }

        if (!wholeWord) {
            return text.toLowerCase(Locale.ROOT).contains(term.toLowerCase(Locale.ROOT));
        }

        String regex = "(?<!" + WORD_CHAR_CLASS + ")" + Pattern.quote(term) + "(?!" + WORD_CHAR_CLASS + ")";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
        return pattern.matcher(text).find();
    }
}
