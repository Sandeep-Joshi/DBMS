package edu.stevens.cs562.DB;

public class GroupingVariableAggregate {
	private String aggregateFunction;
	private String column;
	
	public String getAggregateFunction() {
		return aggregateFunction;
	}
	public void setAggregateFunction(String aggregateFunction) {
		this.aggregateFunction = aggregateFunction;
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
}
