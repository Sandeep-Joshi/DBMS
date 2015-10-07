package edu.stevens.cs562.DB;

import java.util.ArrayList;

public class GroupingVariable {
	private String name;
	private ArrayList<GroupingVariableAggregate> aggregateList = new ArrayList<GroupingVariableAggregate>();
	private String condition;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public ArrayList<GroupingVariableAggregate> getAggregateList() {
		return aggregateList;
	}
	public String getCondition() {
		return condition;
	}
	public void setCondition(String condition) {
		this.condition = condition;
	}
}
