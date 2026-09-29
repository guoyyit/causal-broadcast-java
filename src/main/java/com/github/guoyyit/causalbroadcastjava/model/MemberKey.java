package com.github.guoyyit.causalbroadcastjava.model;

/**
 * A member identity qualified by its generation. The same participant id may
 * rejoin after leaving; each join gets a fresh generation so that its vector
 * clock component never collides with an earlier incarnation.
 */
public record MemberKey(String id, int gen) {

    public static MemberKey parse(String key) {
        int separator = key.lastIndexOf('#');
        if (separator < 0) {
            throw new IllegalArgumentException("invalid member key: " + key);
        }
        String id = key.substring(0, separator);
        int parsedGen = Integer.parseInt(key.substring(separator + 1));
        return new MemberKey(id, parsedGen);
    }

    public String key() {
        return id + "#" + gen;
    }
}
