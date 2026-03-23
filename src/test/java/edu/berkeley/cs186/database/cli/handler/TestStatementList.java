package edu.berkeley.cs186.database.cli.handler;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieSQLLexer;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser;
import edu.berkeley.cs186.database.cli.handler.StatementListHandler;
import edu.berkeley.cs186.database.cli.handler.StatementType;
import org.antlr.v4.runtime.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import static org.junit.Assert.assertEquals;

public class TestStatementList {
    private static final String TestDir = "testSelectClause";
    private Database db;
    private String filename;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder(TestDir);
        this.filename = testDir.getAbsolutePath();
        this.db = new Database(filename, 32);
        this.db.setWorkMem(16);
        try {
            db.loadDemo();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @After
    public void afterEach() {
        try(Transaction t = this.db.beginTransaction()) {
            t.dropAllTables();
        }
        this.db.close();
    }

    public StatementListHandler parse(String input) {
        CharStream charStream = CharStreams.fromString(input);
        RookieSQLLexer lexer = new RookieSQLLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        RookieSQLParser parser = new RookieSQLParser(tokens);
        StatementListHandler handler = new StatementListHandler(db, System.out);
        handler.visit(parser.sqlStmtList());
        return handler;
    }

    @Test
    public void testSimpleQuery() {
        StatementListHandler handler = parse(
                "SELECT * FROM Students;"
        );
        assertEquals(1, handler.statementHandlers.size());
        assertEquals(StatementType.SELECT, handler.statementHandlers.get(0).getType());
    }

    @Test
    public void testSemicolonsA() {
        StatementListHandler handler = parse(
                ";;;SELECT * FROM Students;"
        );
        assertEquals(1, handler.statementHandlers.size());
        assertEquals(StatementType.SELECT, handler.statementHandlers.get(0).getType());
    }

    @Test
    public void testSemicolonsB() {
        StatementListHandler handler = parse(
                "SELECT * FROM Students;;;;;;;"
        );
        assertEquals(1, handler.statementHandlers.size());
        assertEquals(StatementType.SELECT, handler.statementHandlers.get(0).getType());
    }

    @Test
    public void testSemicolonsC() {
        StatementListHandler handler = parse(
                ";;; ;; ; SELECT * FROM Students;;\n;;\t;;;"
        );
        assertEquals(1, handler.statementHandlers.size());
        assertEquals(StatementType.SELECT, handler.statementHandlers.get(0).getType());
    }

    @Test
    public void testSemicolonsD() {
        StatementListHandler handler = parse(
                ";  ;; ;;\t; SELECT * FROM Students;;\n\n;;;EXPLAIN SELECT * FROM Students;;"
        );
        assertEquals(2, handler.statementHandlers.size());
        assertEquals(StatementType.SELECT, handler.statementHandlers.get(0).getType());
        assertEquals(StatementType.EXPLAIN, handler.statementHandlers.get(1).getType());
    }
}
