package cn.xpleaf.spark.antlr;

import cn.xpleaf.spark.antlr.codegen.CalculatorBaseVisitor;
import cn.xpleaf.spark.antlr.codegen.CalculatorParser;

public class MyCalculatorVisitor extends CalculatorBaseVisitor<Object> {

    @Override
    public Object visitMultOrDiv(CalculatorParser.MultOrDivContext ctx) {
        Object obj0 = ctx.expr(0).accept(this);
        Object obj1 = ctx.expr(1).accept(this);
        if ("*".equals(ctx.getChild(1).getText())) {
            return (Float) obj0 * (Float) obj1;
        } else if ("/".equals(ctx.getChild(1).getText())) {
            return (Float) obj0 / (Float) obj1;
        }
        return 0f;
    }

    @Override
    public Object visitAddOrSubtract(CalculatorParser.AddOrSubtractContext ctx) {
        Object obj0 = ctx.expr(0).accept(this);
        Object obj1 = ctx.expr(1).accept(this);
        if ("+".equals(ctx.getChild(1).getText())) {
            return (Float) obj0 + (Float) obj1;
        } else if ("-".equals(ctx.getChild(1).getText())) {
            return (Float) obj0 - (Float) obj1;
        }
        return 0f;
    }

    @Override
    public Object visitFloat(CalculatorParser.FloatContext ctx) {
        return Float.parseFloat(ctx.getText());
    }

    @Override
    public Object visitParenExpr(CalculatorParser.ParenExprContext ctx) {
        return ctx.expr().accept(this);
    }
}
