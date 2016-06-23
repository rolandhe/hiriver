package com.hiriver.unbiz.mysql.lib.filter.impl;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.hiriver.unbiz.mysql.lib.filter.TableFilter;

/**
 * 支持黑白名单的过滤实现。<br>
 * <ul>
 * <li>按照表名进行过滤时，表名格式为database.table（可以为正则），以逗号分隔</li>
 * <li>当白名单和黑名单同时存在时,只有不在黑名单中同时在白名单中存在的才起作用</li>
 * </ul>
 * <b>e.g</b>,在properties文件中描述<br>
 * <ul>
 * <li>白名单：filert_white=test.account,test.user_sharding*</li>
 * <li>白名单：filert_black=test.*bak</li>
 * </ul>
 * 
 * @author hexiufeng
 *
 */
public class BlackWhiteNameListTableFilter implements TableFilter {
    public static final String FILTER_SEPARATOR = ",";
    private String tableWhite;
    private String tableBlack;

    private Set<Pattern> tableWhitePatternSet = new HashSet<Pattern>();
    private Set<Pattern> tableBlackPatternSet = new HashSet<Pattern>();
    private Set<String> tableWhiteEqualsSet = new HashSet<String>();
    private Set<String> tableBlackEqualsSet = new HashSet<String>();

    public String getTableWhite() {
        return tableWhite;
    }

    public void setTableWhite(String tableWhite) {
        this.tableWhite = tableWhite;
        if (StringUtils.isNotBlank(this.tableWhite)) {
            String[] filterArr = this.tableWhite.split(FILTER_SEPARATOR);
            for (String filterStr : filterArr) {
//                if (isRegex(filterStr)) {
                    tableWhitePatternSet.add(Pattern.compile(filterStr.trim()));
//                } else {
//                    tableWhiteEqualsSet.add(filterStr);
//                }
            }
        }
    }

    public String getTableBlack() {
        return tableBlack;
    }

    public void setTableBlack(String tableBlack) {
        this.tableBlack = tableBlack;
        if (StringUtils.isNotBlank(this.tableBlack)) {
            String[] filterArr = this.tableBlack.split(FILTER_SEPARATOR);
            for (String filterStr : filterArr) {
//                if (isRegex(filterStr)) {
                    tableBlackPatternSet.add(Pattern.compile(filterStr.trim()));
//                } else {
//                    tableBlackEqualsSet.add(filterStr);
//                }
            }
        }
    }

    
    
    @Override
    public boolean filter(String dbName, String tableName) {
        String fullTableName = dbName + "." + tableName;
        if (isBlack(fullTableName)) {
            return false;
        }
        return canPassWhite(fullTableName);
    }

    /**
     * 是否可以通过白名单限制
     * 
     * @param fullTableName table name
     * @return boolean
     */
    private boolean canPassWhite(String fullTableName) {
        boolean hasWhiteSetting =
                tableWhiteEqualsSet.size() > 0
                        || tableWhitePatternSet.size() > 0;
        if (!hasWhiteSetting) {
            return true;
        }
        for (String name : tableWhiteEqualsSet) {
            if (name.equals(fullTableName)) {
                return true;
            }
        }
        for (Pattern pattern : tableWhitePatternSet) {
            Matcher matcher = pattern.matcher(fullTableName);
            if (matcher.find()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 表是否是黑名单表
     * 
     * @param fullTableName fullTableName
     * @return boolean
     */
    private boolean isBlack(String fullTableName) {
        for (String name : tableBlackEqualsSet) {
            if (name.equals(fullTableName)) {
                return true;
            }
        }
        if (tableBlackPatternSet != null && tableBlackPatternSet.size() > 0) {
            for (Pattern pattern : tableBlackPatternSet) {
                Matcher matcher = pattern.matcher(fullTableName);
                if (matcher.find()) {
                    return true;
                }
            }
        }
        return false;
    }


    /**
     * pattern是否是正则表达时
     * 
     * @param pattern pattern
     * @return boolean
     */
//    private boolean isRegex(String pattern) {
//        char[] array = pattern.toCharArray();
//
//        for (char c : array) {
//            if (c == '^' || c == '$' || c == '*' || c == '?') {
//                return true;
//            }
//        }
//        return false;
//    }


}
