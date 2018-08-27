package cf.janga.cwql.planner

import cf.janga.cwql.parser._
import cf.janga.cwql.planner.InsertPlanner._
import cf.janga.cwql.planner.QueryPlanner._
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}

case class CwqlPlan(steps: Seq[Step])

case class UnorderedProjection(originalProjection: Projection, order: Int)
private case class GroupedProjections(namespace: Namespace, metric: String, projections: Seq[UnorderedProjection])

sealed trait PlannerError
case object StartTimeAfterEndTime extends PlannerError
case class UnmatchedProjection(projection: Projection) extends PlannerError
case class UnmatchedFilter(booleanExpression: SimpleBooleanExpression) extends PlannerError
case class ProjectionAliasAlreadyInUse(projection: Projection) extends PlannerError

class Planner(implicit awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()) {

  def plan(cwqlStatement: CwqlStatement): Either[PlannerError, CwqlPlan] = cwqlStatement match {
    case query: Query => planQuery(query)
    case insert: Insert => planInsert(insert)
  }
}

