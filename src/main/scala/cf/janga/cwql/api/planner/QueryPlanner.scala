package cf.janga.cwql.api.planner

import cf.janga.cwql.api.parser._
import cf.janga.cwql.api.planner.CwQueryConversions._
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.cloudwatch.model.{Dimension, GetMetricStatisticsRequest}
import org.joda.time.format.ISODateTimeFormat

import scala.annotation.tailrec
import scala.collection.JavaConverters._

private[planner] object QueryPlanner {

  def planQuery(query: Query)(implicit awsCredentialsProvider: AWSCredentialsProvider): Either[PlannerError, CwqlPlan] = {
    for {
      _ <- preCheck(query)
      projectionsPerMetric <- groupProjectionsPerMetric(query.projections, query.namespaces, Map.empty, 0)
      cwRequestStep <- planCwRequestStep(projectionsPerMetric, query.selectionOption, query.between, query.period, Seq.empty)
    } yield {
      CwqlPlan(Seq(cwRequestStep, OrderByStep(None)))
    }
  }

  private def preCheck(query: Query): Either[PlannerError, Unit] = {
    for {
      _ <- checkForDuplicateProjectionAliases(query.projections, Set.empty)
    } yield ()
  }

  @tailrec
  private def checkForDuplicateProjectionAliases(projections: Seq[Projection], aliasSet: Set[String]): Either[PlannerError, Unit] = projections.headOption match {
    case Some(projection) => {
      projection.alias match {
        case Some(alias) if aliasSet.contains(alias) => Left(ProjectionAliasAlreadyInUse(projection))
        case Some(alias) => checkForDuplicateProjectionAliases(projections.tail, aliasSet ++ Set(alias))
        case None => checkForDuplicateProjectionAliases(projections.tail, aliasSet)
      }
    }
    case None => Right(())
  }

  private def planCwRequestStep(groupedProjections: Seq[GroupedProjections],
                                selectionOption: Option[Selection],
                                between: Between, period: Period,
                                currentRequests: Iterable[GetMetricStatisticsRequestForProjections])
                               (implicit awsCredentialsProvider: AWSCredentialsProvider): Either[PlannerError, GetMetricStatisticsStep] = {
    groupedProjections.headOption match {
      case Some(groupedProjection) => {
        val formatter = ISODateTimeFormat.dateTimeNoMillis()
        val startTime = formatter.parseDateTime(between.startTime)
        val endTime = formatter.parseDateTime(between.endTime)
        if (startTime.isAfter(endTime)) {
          Left(StartTimeAfterEndTime)
        } else {
          val cloudWatchRequest = new GetMetricStatisticsRequest()
          cloudWatchRequest.setMetricName(groupedProjection.metric)
          cloudWatchRequest.setStartTime(startTime.toDate)
          cloudWatchRequest.setEndTime(endTime.toDate)
          cloudWatchRequest.setNamespace(groupedProjection.namespace.value)
          cloudWatchRequest.setPeriod(period.value)
          getStatisticsAndDimensions(groupedProjection.namespace, groupedProjection.projections, selectionOption, Seq.empty, Seq.empty).flatMap {
            case (projectionStatistics, dimensions, newSelectionOption) => {
              cloudWatchRequest.setStatistics(projectionStatistics.map(_.originalProjection.statistic.toAwsStatistic).asJava)
              cloudWatchRequest.setDimensions(dimensions.asJava)
              planCwRequestStep(groupedProjections.tail, newSelectionOption, between, period, currentRequests ++ Iterable(GetMetricStatisticsRequestForProjections(cloudWatchRequest, projectionStatistics)))
            }
          }
        }
      }
      case None => {
        selectionOption.fold[Either[PlannerError, GetMetricStatisticsStep]](Right(GetMetricStatisticsStep(awsCredentialsProvider, currentRequests.toSeq))) {
          selection => Left(UnmatchedFilter(selection.booleanExpression.simpleBooleanExpression))
        }
      }
    }
  }

  def getStatisticsAndDimensions(namespace: Namespace, unorderedProjections: Seq[UnorderedProjection], selectionOption: Option[Selection],
                                 projectionStatistics: Seq[UnorderedProjection], dimensions: Seq[Dimension]): Either[PlannerError, (Seq[UnorderedProjection], Seq[Dimension], Option[Selection])] = unorderedProjections match {
    case unorderedProjection :: remainder => {
      val newProjectionStatistics = projectionStatistics ++ Seq(unorderedProjection)
      selectionOption match {
        case Some(selection) => {
          val (newSelectionOption, newDimensions) = getDimensionsFromSelection(namespace, unorderedProjection.originalProjection, selection)
          getStatisticsAndDimensions(namespace, remainder, newSelectionOption, newProjectionStatistics, dimensions ++ newDimensions)
        }
        case None => {
          getStatisticsAndDimensions(namespace, remainder, selectionOption, newProjectionStatistics, dimensions)
        }
      }
    }
    case Nil => Right((projectionStatistics, dimensions, selectionOption))
  }

  private def getDimensionsFromSelection(namespace: Namespace, projection: Projection, selection: Selection): (Option[Selection], Seq[Dimension]) = {
    def getDimensionsFromSelectionRec(namespace: Namespace, projection: Projection,
                                      booleanExpressions: Seq[(BooleanOperator, SimpleBooleanExpression)],
                                      unmatchedExpressions: Seq[(BooleanOperator, SimpleBooleanExpression)],
                                      dimensions: Seq[Dimension]): (Option[Selection], Seq[Dimension]) = {
      booleanExpressions match {
        case booleanExpression :: remainingExpressions => {
          val dimensionOption = booleanExpression._2.toDimension(namespace, projection)
          dimensionOption match {
            case Some(dimension) => {
              val newSelection = selection.booleanExpression.nested.headOption.map(nested => Selection(BooleanExpression(nested._2, selection.booleanExpression.nested.tail)))
              getDimensionsFromSelectionRec(namespace, projection, remainingExpressions, unmatchedExpressions, dimensions ++ Seq(dimension))
            }
            case None => {
              getDimensionsFromSelectionRec(namespace, projection, remainingExpressions, unmatchedExpressions ++ Seq(booleanExpression), dimensions)
            }
          }
        }
        case Nil => {
          val newSelectionOption = unmatchedExpressions.headOption.map(unmatchedExpression => Selection(BooleanExpression(unmatchedExpression._2, unmatchedExpressions.tail)))
          (newSelectionOption, dimensions)
        }
      }
    }

    getDimensionsFromSelectionRec(namespace, projection, Seq((And, selection.booleanExpression.simpleBooleanExpression)) ++ selection.booleanExpression.nested, Seq.empty, Seq.empty)
  }

  @tailrec
  private def groupProjectionsPerMetric(projections: Seq[Projection],
                                        namespaces: Seq[Namespace],
                                        groupedProjectionsMap: Map[String, GroupedProjections],
                                        order: Int): Either[PlannerError, Seq[GroupedProjections]] = projections.headOption match {
    case Some(projection) => {
      val matchingNamespace =
        namespaces.collectFirst {
          case namespace: Namespace if namespace.aliasOption.isDefined && projection.namespaceAlias.isDefined && projection.namespaceAlias.get == namespace.aliasOption.get => {
            (s"${namespace.value}-${namespace.aliasOption.get}-${projection.metric}", namespace)
          }
          case namespace: Namespace if namespace.aliasOption.isEmpty && projection.namespaceAlias.isEmpty && namespaces.size == 1 => {
            (s"${namespace.value}-${projection.metric}", namespace)
          }
        }
      matchingNamespace match {
        case Some((key, namespace)) => {
          groupedProjectionsMap.get(key) match {
            case None => {
              val unorderedProjection = UnorderedProjection(projection, order)
              val newMap = groupedProjectionsMap + (key -> GroupedProjections(namespace, projection.metric, Seq(unorderedProjection)))
              groupProjectionsPerMetric(projections.tail, namespaces, newMap, order + 1)
            }
            case Some(existingGroupedProjections) => {
              val unorderedProjection = UnorderedProjection(projection, order)
              val newProjections = existingGroupedProjections.projections ++ Seq(unorderedProjection)
              val newMap = groupedProjectionsMap + (key -> GroupedProjections(namespace, projection.metric, newProjections))
              groupProjectionsPerMetric(projections.tail, namespaces, newMap, order + 1)
            }
          }
        }
        case None => Left(UnmatchedProjection(projection))
      }
    }
    case None => Right(groupedProjectionsMap.values.toSeq)
  }
}
