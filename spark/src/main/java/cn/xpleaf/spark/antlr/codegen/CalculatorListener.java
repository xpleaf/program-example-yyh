// Generated from /Users/yyh/projects/common/program-example-yyh/spark/src/main/java/cn/xpleaf/spark/antlr/Calculator.g4 by ANTLR 4.8
package cn.xpleaf.spark.antlr.codegen;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link CalculatorParser}.
 */
public interface CalculatorListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link CalculatorParser#line}.
	 * @param ctx the parse tree
	 */
	void enterLine(CalculatorParser.LineContext ctx);
	/**
	 * Exit a parse tree produced by {@link CalculatorParser#line}.
	 * @param ctx the parse tree
	 */
	void exitLine(CalculatorParser.LineContext ctx);
	/**
	 * Enter a parse tree produced by the {@code multOrDiv}
	 * labeled alternative in {@link CalculatorParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterMultOrDiv(CalculatorParser.MultOrDivContext ctx);
	/**
	 * Exit a parse tree produced by the {@code multOrDiv}
	 * labeled alternative in {@link CalculatorParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitMultOrDiv(CalculatorParser.MultOrDivContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addOrSubtract}
	 * labeled alternative in {@link CalculatorParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterAddOrSubtract(CalculatorParser.AddOrSubtractContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addOrSubtract}
	 * labeled alternative in {@link CalculatorParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitAddOrSubtract(CalculatorParser.AddOrSubtractContext ctx);
	/**
	 * Enter a parse tree produced by the {@code float}
	 * labeled alternative in {@link CalculatorParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterFloat(CalculatorParser.FloatContext ctx);
	/**
	 * Exit a parse tree produced by the {@code float}
	 * labeled alternative in {@link CalculatorParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitFloat(CalculatorParser.FloatContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenExpr}
	 * labeled alternative in {@link CalculatorParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterParenExpr(CalculatorParser.ParenExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenExpr}
	 * labeled alternative in {@link CalculatorParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitParenExpr(CalculatorParser.ParenExprContext ctx);
}