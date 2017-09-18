package com.timgroup.eventstore.mysql;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.ResourceBundle;

import com.mchange.v2.log.MLevel;
import com.mchange.v2.log.MLog;
import com.mchange.v2.log.MLogger;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slf4jMLog extends MLog {
    private final ILoggerFactory factory = LoggerFactory.getILoggerFactory();

    @Override public MLogger getMLogger(String name) {
        return new Impl(factory.getLogger(name));
    }

    @Override public MLogger getMLogger(@SuppressWarnings("rawtypes") Class cl) {
        return new Impl(factory.getLogger(cl.getName()));
    }

    @Override public MLogger getMLogger() {
        return new Impl(factory.getLogger(""));
    }

    private static final class Impl implements MLogger {
        private final Logger underlying;

        public Impl(Logger underlying) {
            this.underlying = underlying;
        }

        @Override public void severe(String msg) {
            underlying.error(msg);
        }

        @Override public void warning(String msg) {
            underlying.warn(msg);
        }

        @Override public void info(String msg) {
            underlying.info(msg);
        }

        @Override public void fine(String msg) {
            underlying.debug(msg);
        }

        @Override public void finer(String msg) {
            underlying.debug(msg);
        }

        @Override public void finest(String msg) {
            underlying.debug(msg);
        }

        @Override public boolean isLoggable(MLevel l) {
            if (l.intValue() >= MLevel.SEVERE.intValue()) {
                return underlying.isErrorEnabled();
            } else if (l.intValue() >= MLevel.WARNING.intValue()) {
                return underlying.isWarnEnabled();
            } else if (l.intValue() >= MLevel.INFO.intValue()) {
                return underlying.isInfoEnabled();
            } else {
                return underlying.isDebugEnabled();
            }
        }

        @Override public void log(MLevel l, String msg) {
            if (l.intValue() >= MLevel.SEVERE.intValue()) {
                underlying.error(msg);
            } else if (l.intValue() >= MLevel.WARNING.intValue()) {
                underlying.warn(msg);
            } else if (l.intValue() >= MLevel.INFO.intValue()) {
                underlying.info(msg);
            } else {
                underlying.debug(msg);
            }
        }

        @Override public void log(MLevel l, String msg, Object param) {
            if (l.intValue() >= MLevel.SEVERE.intValue()) {
                if (underlying.isErrorEnabled()) {
                    underlying.error(msg.replace("{0}", param.toString()));
                }
            } else if (l.intValue() >= MLevel.WARNING.intValue()) {
                if (underlying.isWarnEnabled()) {
                    underlying.warn(msg.replace("{0}", param.toString()));
                }
            } else if (l.intValue() >= MLevel.INFO.intValue()) {
                if (underlying.isInfoEnabled()) {
                    underlying.info(msg.replace("{0}", param.toString()));
                }
            } else {
                if (underlying.isDebugEnabled()) {
                    underlying.debug(msg.replace("{0}", param.toString()));
                }
            }
        }

        @Override public void log(MLevel l, String msg, Object[] params) {
            if (l.intValue() >= MLevel.SEVERE.intValue()) {
                if (underlying.isErrorEnabled()) {
                    underlying.error(MessageFormat.format(msg, params));
                }
            } else if (l.intValue() >= MLevel.WARNING.intValue()) {
                if (underlying.isWarnEnabled()) {
                    underlying.warn(MessageFormat.format(msg, params));
                }
            } else if (l.intValue() >= MLevel.INFO.intValue()) {
                if (underlying.isInfoEnabled()) {
                    underlying.info(MessageFormat.format(msg, params));
                }
            } else {
                if (underlying.isDebugEnabled()) {
                    underlying.debug(MessageFormat.format(msg, params));
                }
            }
        }

        @Override public void log(MLevel l, String msg, Throwable t) {
            if (l.intValue() >= MLevel.SEVERE.intValue()) {
                underlying.error(msg, t);
            } else if (l.intValue() >= MLevel.WARNING.intValue()) {
                underlying.warn(msg, t);
            } else if (l.intValue() >= MLevel.INFO.intValue()) {
                underlying.info(msg, t);
            } else {
                underlying.debug(msg, t);
            }
        }

        @Override public ResourceBundle getResourceBundle() {
            throw new UnsupportedOperationException();
        }

        @Override public String getResourceBundleName() {
            throw new UnsupportedOperationException();
        }

        @Override public void setFilter(Object java14Filter) throws SecurityException {
        }

        @Override public Object getFilter() {
            return null;
        }

        @Override public void logp(MLevel l, String srcClass, String srcMeth, String msg) {
            log(l, msg);
        }

        @Override public void logp(MLevel l, String srcClass, String srcMeth, String msg, Object param) {
            log(l, msg, param);
        }

        @Override public void logp(MLevel l, String srcClass, String srcMeth, String msg, Object[] params) {
            log(l, msg, params);
        }

        @Override public void logp(MLevel l, String srcClass, String srcMeth, String msg, Throwable t) {
            log(l, msg, t);
        }

        @Override public void logrb(MLevel l, String srcClass, String srcMeth, String rb, String msg) {
            log(l, msg);
        }

        @Override public void logrb(MLevel l, String srcClass, String srcMeth, String rb, String msg, Object param) {
            log(l, msg, param);
        }

        @Override public void logrb(MLevel l, String srcClass, String srcMeth, String rb, String msg, Object[] params) {
            log(l, msg, params);
        }

        @Override public void logrb(MLevel l, String srcClass, String srcMeth, String rb, String msg, Throwable t) {
            log(l, msg, t);
        }

        @Override public void entering(String srcClass, String srcMeth) {
            underlying.trace("entering {} {}", srcClass, srcMeth);
        }

        @Override public void entering(String srcClass, String srcMeth, Object param) {
            underlying.trace("entering {} {} {}", srcClass, srcMeth, param);
        }

        @Override public void entering(String srcClass, String srcMeth, Object[] params) {
            underlying.trace("entering {} {} {}", srcClass, srcMeth, Arrays.asList(params));
        }

        @Override public void exiting(String srcClass, String srcMeth) {
            underlying.trace("exiting {} {}", srcClass, srcMeth);
        }

        @Override public void exiting(String srcClass, String srcMeth, Object result) {
            underlying.trace("exiting {} {} {}", srcClass, srcMeth, result);
        }

        @Override public void throwing(String srcClass, String srcMeth, Throwable t) {
            if (t != null) {
                underlying.trace("exiting {} {}", srcClass, srcMeth, t);
            } else {
                underlying.trace("exiting {} {}", srcClass, srcMeth);
            }
        }

        @Override public void config(String msg) {
        }

        @Override public void setLevel(MLevel l) throws SecurityException {
        }

        @Override public MLevel getLevel() {
            if (underlying.isDebugEnabled()) {
                return MLevel.FINEST;
            } else if (underlying.isInfoEnabled()) {
                return MLevel.INFO;
            } else if (underlying.isWarnEnabled()) {
                return MLevel.WARNING;
            } else if (underlying.isErrorEnabled()) {
                return MLevel.SEVERE;
            } else {
                return MLevel.OFF;
            }
        }

        @Override public String getName() {
            return underlying.getName();
        }

        @Override public void addHandler(Object h) throws SecurityException {
            throw new UnsupportedOperationException();
        }

        @Override public void removeHandler(Object h) throws SecurityException {
            throw new UnsupportedOperationException();
        }

        @Override public Object[] getHandlers() {
            throw new UnsupportedOperationException();
        }

        @Override public void setUseParentHandlers(boolean uph) {
            throw new UnsupportedOperationException();
        }

        @Override public boolean getUseParentHandlers() {
            throw new UnsupportedOperationException();
        }
    }
}
