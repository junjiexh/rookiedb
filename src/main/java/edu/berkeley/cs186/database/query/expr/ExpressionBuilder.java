package edu.berkeley.cs186.database.query.expr;

import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.cli.parser.RookieSQLParser.*;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;

import java.util.ArrayList;
import java.util.List;

public class ExpressionBuilder {

    public static Expression build(ExpressionContext ctx) {
        return buildOr(ctx.orExpr());
    }

    private static Expression buildOr(OrExprContext ctx) {
        List<AndExprContext> andExprs = ctx.andExpr();
        if (andExprs.size() == 1) return buildAnd(andExprs.get(0));
        Expression[] children = new Expression[andExprs.size()];
        for (int i = 0; i < andExprs.size(); i++) {
            children[i] = buildAnd(andExprs.get(i));
        }
        return Expression.or(children);
    }

    private static Expression buildAnd(AndExprContext ctx) {
        List<NotExprContext> notExprs = ctx.notExpr();
        if (notExprs.size() == 1) return buildNot(notExprs.get(0));
        Expression[] children = new Expression[notExprs.size()];
        for (int i = 0; i < notExprs.size(); i++) {
            children[i] = buildNot(notExprs.get(i));
        }
        return Expression.and(children);
    }

    private static Expression buildNot(NotExprContext ctx) {
        int notCount = ctx.notOperator().size();
        Expression expr = buildComparison(ctx.comparisonExpr());
        if (notCount % 2 != 0) {
            expr = Expression.not(expr);
        }
        return expr;
    }

    private static Expression buildComparison(ComparisonExprContext ctx) {
        List<AdditiveExprContext> additiveExprs = ctx.additiveExpr();
        List<ComparisonOperatorContext> operators = ctx.comparisonOperator();
        if (additiveExprs.size() == 1) return buildAdditive(additiveExprs.get(0));
        Expression prev = buildAdditive(additiveExprs.get(0));
        for (int i = 0; i < operators.size(); i++) {
            Expression curr = buildAdditive(additiveExprs.get(i + 1));
            prev = Expression.compare(operators.get(i).getText(), prev, curr);
        }
        return prev;
    }

    private static Expression buildAdditive(AdditiveExprContext ctx) {
        List<MultiplicativeExprContext> multExprs = ctx.multiplicativeExpr();
        List<AdditiveOperatorContext> operators = ctx.additiveOperator();
        if (multExprs.size() == 1) return buildMultiplicative(multExprs.get(0));
        List<Character> ops = new ArrayList<>();
        for (AdditiveOperatorContext op : operators) {
            ops.add(op.getText().charAt(0));
        }
        Expression[] children = new Expression[multExprs.size()];
        for (int i = 0; i < multExprs.size(); i++) {
            children[i] = buildMultiplicative(multExprs.get(i));
        }
        return Expression.additive(ops, children);
    }

    private static Expression buildMultiplicative(MultiplicativeExprContext ctx) {
        List<PrimaryExprContext> primaryExprs = ctx.primaryExpr();
        List<MultiplicativeOperatorContext> operators = ctx.multiplicativeOperator();
        if (primaryExprs.size() == 1) return buildPrimary(primaryExprs.get(0));
        List<Character> ops = new ArrayList<>();
        for (MultiplicativeOperatorContext op : operators) {
            ops.add(op.getText().charAt(0));
        }
        Expression[] children = new Expression[primaryExprs.size()];
        for (int i = 0; i < primaryExprs.size(); i++) {
            children[i] = buildPrimary(primaryExprs.get(i));
        }
        return Expression.multiplicative(ops, children);
    }

    private static Expression buildPrimary(PrimaryExprContext ctx) {
        if (ctx instanceof PrimaryLiteralContext litCtx) {
            DataBox value = PrettyPrinter.parseLiteral(litCtx.literal().getText());
            return Expression.literal(value);
        } else if (ctx instanceof PrimaryColumnContext colCtx) {
            String columnName = colCtx.columnName().getText();
            return Expression.column(columnName);
        } else if (ctx instanceof PrimaryParenContext parenCtx) {
            Expression expr = build(parenCtx.expression());
            expr.needsParentheses = true;
            return expr;
        } else if (ctx instanceof PrimaryUnaryContext unaryCtx) {
            Expression child = buildPrimary(unaryCtx.primaryExpr());
            String opText = unaryCtx.additiveOperator().getText();
            if (opText.equals("-")) {
                return Expression.negate(child);
            }
            return child;
        } else if (ctx instanceof PrimaryFunctionCallContext funcCtx) {
            return buildFunctionCall(funcCtx.functionCallExpr());
        }
        throw new UnsupportedOperationException("Unknown primary expression type");
    }

    private static Expression buildFunctionCall(FunctionCallExprContext ctx) {
        String functionName = ctx.identifier().getText();
        List<ExpressionContext> exprContexts = ctx.expression();
        List<Expression> operands = new ArrayList<>();
        for (ExpressionContext ec : exprContexts) {
            operands.add(build(ec));
        }

        if (operands.size() == 1 || (functionName.toUpperCase().equals("COUNT"))) {
            Expression child;
            if (operands.size() == 1) child = operands.get(0);
            else child = Expression.literal(new IntDataBox(0));
            Expression agg = Expression.function(functionName, child);
            if (agg != null) return agg;
        }
        Expression[] children = new Expression[operands.size()];
        for (int i = 0; i < operands.size(); ++i) {
            children[i] = operands.get(i);
        }
        Expression reg = Expression.function(functionName, children);
        if (reg != null) return reg;
        throw new UnsupportedOperationException("Unknown function `" + functionName + "`" + " (with " + operands.size() + " argument(s))");
    }
}
