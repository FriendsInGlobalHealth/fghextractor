package tz.co.juutech.extractor;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/21/21.
 */
public class TableReferencingAnother {
    private String table;
    private String columnName;

    public TableReferencingAnother(String table, String columnName) {
        assert table != null;
        assert columnName != null;

        this.table = table;
        this.columnName = columnName;
    }

    public String getTable() {
        return table;
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableReferencingAnother)) return false;

        TableReferencingAnother that = (TableReferencingAnother) o;

        if (!getTable().equals(that.getTable())) return false;
        return getColumnName().equals(that.getColumnName());
    }

    @Override
    public int hashCode() {
        int result = getTable().hashCode();
        result = 31 * result + getColumnName().hashCode();
        return result;
    }
}
