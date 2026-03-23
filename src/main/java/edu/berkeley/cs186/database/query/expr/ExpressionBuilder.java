package edu.berkeley.cs186.database.query.expr;

import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.cli.parser.RookieSQLBaseVisitor;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.*;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import java.util.ArrayList;
import java.util.List;

public class ExpressionBuilder extends RookieSQLBaseVisitor<Expression> {

    public static Expression build(ExpressionContext ctx) {
        return new ExpressionBuilder().visit(ctx);
    }

    @Override
    public Expression visitOrExpr(OrExprContext ctx) {
        List<AndExprContext> children = ctx.andExpr();
        if (children.size() == 1) return visit(children.get(0));
        Expression[] operands = new Expression[children.size()];
        for (int i = 0; i < children.size(); i++) {
            operands[i] = visit(children.get(i));
        }
        return Expression.or(operands);
    }

    @Override
    public Expression visitAndExpr(AndExprContext ctx) {
        List<NotExprContext> children = ctx.notExpr();
        if (children.size() == 1) return visit(children.get(0));
        Expression[] operands = new Expression[children.size()];
        for (int i = 0; i < children.size(); i++) {
            operands[i] = visit(children.get(i));
        }
        return Expression.and(operands);
    }

    @Override
    public Expression visitNotExpr(NotExprContext ctx) {
        Expression expr = visit(ctx.comparisonExpr());
        if (ctx.notOperator().size() % 2 != 0) {
            expr = Expression.not(expr);
        }
        return expr;
    }

    @Override
    public Expression visitComparisonExpr(ComparisonExprContext ctx) {
        List<AdditiveExprContext> operands = ctx.additiveExpr();
        List<ComparisonOperatorContext> operators = ctx.comparisonOperator();
        if (operands.size() == 1) return visit(operands.get(0));
        Expression prev = visit(operands.get(0));
        for (int i = 0; i < operators.size(); i++) {
            prev = Expression.compare(
                operators.get(i).getText(),
                prev,
                visit(operands.get(i + 1))
            );
        }
        return prev;
    }

    @Override
    public Expression visitAdditiveExpr(AdditiveExprContext ctx) {
        List<MultiplicativeExprContext> operands = ctx.multiplicativeExpr();
        List<AdditiveOperatorContext> operators = ctx.additiveOperator();
        if (operands.size() == 1) return visit(operands.get(0));
        List<Character> ops = new ArrayList<>();
        for (AdditiveOperatorContext op : operators) {
            ops.add(op.getText().charAt(0));
        }
        Expression[] children = new Expression[operands.size()];
        for (int i = 0; i < operands.size(); i++) {
            children[i] = visit(operands.get(i));
        }
        return Expression.additive(ops, children);
    }

    @Override
    public Expression visitMultiplicativeExpr(MultiplicativeExprContext ctx) {
        List<PrimaryExprContext> operands = ctx.primaryExpr();
        List<MultiplicativeOperatorContext> operators =
            ctx.multiplicativeOperator();
        if (operands.size() == 1) return visit(operands.get(0));
        List<Character> ops = new ArrayList<>();
        for (MultiplicativeOperatorContext op : operators) {
            ops.add(op.getText().charAt(0));
        }
        Expression[] children = new Expression[operands.size()];
        for (int i = 0; i < operands.size(); i++) {
            children[i] = visit(operands.get(i));
        }
        return Expression.multiplicative(ops, children);
    }

    // Alternative labels on primaryExpr — ANTLR4 auto-dispatches to these methods

    @Override
    public Expression visitPrimaryLiteral(PrimaryLiteralContext ctx) {
        DataBox value = PrettyPrinter.parseLiteral(ctx.literal().getText());
        return Expression.literal(value);
    }

    @Override
    public Expression visitPrimaryColumn(PrimaryColumnContext ctx) {
        return Expression.column(ctx.columnName().getText());
    }

    @Override
    public Expression visitPrimaryParen(PrimaryParenContext ctx) {
        Expression expr = visit(ctx.expression());
        expr.needsParentheses = true;
        return expr;
    }

    @Override
    public Expression visitPrimaryUnary(PrimaryUnaryContext ctx) {
        Expression child = visit(ctx.primaryExpr());
        if (ctx.additiveOperator().getText().equals("-")) {
            return Expression.negate(child);
        }
        return child;
    }

    @Override
    public Expression visitPrimaryFunctionCall(PrimaryFunctionCallContext ctx) {
        return visitFunctionCallExpr(ctx.functionCallExpr());
    }

    @Override
    public Expression visitFunctionCallExpr(FunctionCallExprContext ctx) {
        String functionName = ctx.identifier().getText();
        List<Expression> operands = new ArrayList<>();
        for (ExpressionContext ec : ctx.expression()) {
            operands.add(visit(ec));
        }

        if (ctx.STAR() != null) {
            Expression child = Expression.literal(new IntDataBox(0));
            Expression agg = Expression.function(functionName, child);
            if (agg != null) return agg;
        }

        if (
            operands.size() == 1 || functionName.toUpperCase().equals("COUNT")
        ) {
            Expression child;
            if (operands.size() == 1) child = operands.get(0);
            else child = Expression.literal(new IntDataBox(0));
            Expression agg = Expression.function(functionName, child);
            if (agg != null) return agg;
        }
        Expression[] children = operands.toArray(new Expression[0]);
        Expression reg = Expression.function(functionName, children);
        if (reg != null) return reg;
        throw new UnsupportedOperationException(
            "Unknown function `" +
                functionName +
                "` (with " +
                operands.size() +
                " argument(s))"
        );
    }
}
