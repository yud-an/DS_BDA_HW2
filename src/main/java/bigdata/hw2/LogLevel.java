package bigdata.hw2;

import java.util.Objects;

public class LogLevelHour {
    private int hour;
    private String logLevel;
}

public class LogLevel {
    private int hour;
    private int logLevel;


    public LogLevel( int hour,int logLevel) {
        this.hour = hour;
        this.logLevel = logLevel;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogLevel logLevel1 = (LogLevel) o;
        return logLevel == logLevel1.logLevel &&
                hour == logLevel1.hour;
    }

    @Override
    public int hashCode() {
        return Objects.hash(logLevel, hour);
    }

    @Override
    public String toString() {
        return "" + hour + "," + logLevel;
    }

}
