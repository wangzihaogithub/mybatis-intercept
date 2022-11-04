package com.github.mybatisintercept.util;

import java.util.Objects;
import java.util.TreeSet;

/**
 * Zero memory copy String Method.
 * @author wangzihaogithub
 */
public class Substring implements CharSequence,Comparable<CharSequence>,Cloneable {
    private String source;
    private int begin;
    private int end;
    private int hash;

    public Substring(String source, int begin, int end) {
        this.source = source;
        this.begin = begin;
        this.end = end;
    }

    public Substring(String source) {
        this.source = source;
        this.begin = 0;
        this.end = source.length();
    }

    public void setSource(String source) {
        this.source = source;
        this.hash = 0;
    }

    public void setBegin(int begin) {
        if(this.begin != begin){
            this.hash = 0;
            this.begin = begin;
        }
    }

    public void setEnd(int end) {
        if(this.end < end){
            if(this.hash != 0) {
                int h = hash;
                for (int i = this.end; i < end; i++) {
                    h = 31 * h + source.charAt(i);
                }
                this.hash = h;
            }
            this.end = end;
        }else if(this.end > end){
            this.hash = 0;
            this.end = end;
        }
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0 && end - begin > 0) {
            for (int i = begin; i < end; i++) {
                h = 31 * h + source.charAt(i);
            }
            hash = h;
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Substring) {
            Substring that = (Substring) obj;
            return equals(that.source, that.begin, that.end, source, begin, end);
        }else if(obj instanceof CharSequence) {
            CharSequence that = (CharSequence) obj;
            return equals(that,0,that.length(), source, begin, end);
        }else {
            return false;
        }
    }

    @Override
    public String toString() {
        return source.substring(begin,end);
    }

    @Override
    public int compareTo(CharSequence that) {
        int len1 = this.length();
        int len2 = that.length();
        int min = Math.min(len1, len2);
        int k = 0;
        while (k < min) {
            char c1 = charAt(k);
            char c2 = that.charAt(k);
            if (c1 != c2) {
                return c1 - c2;
            }
            k++;
        }
        return len1 - len2;
    }

    @Override
    public Substring clone() {
        Substring clone = new Substring(source, begin, end);
        clone.hash = this.hash;
        return clone;
    }

    private static boolean equals(CharSequence str1, int begin1, int end1, CharSequence str2, int begin2, int end2){
        if(end2 - begin2 != end1 - begin1){
            return false;
        }
        for (int i = begin1,j = begin2; i < end1; i++,j++) {
            char c1 = str2.charAt(j);
            char c2 = str1.charAt(i);
            if(c1 != c2){
                return false;
            }
        }
        return true;
    }

    @Override
    public int length() {
        return end - begin;
    }

    @Override
    public char charAt(int index) {
        return source.charAt(begin + index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return new Substring(source,begin + start,begin + end);
    }

    public int getBegin() {
        return begin;
    }

    public int getEnd() {
        return end;
    }

    public String getSource() {
        return source;
    }
}