/*
 * The MIT License
 *
 * Copyright 2014 Vincent Latombe
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.sonyericsson.jenkins.plugins.bfa.model;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.jvnet.hudson.test.FailureBuilder;
import org.jvnet.hudson.test.JenkinsRule;
import org.jvnet.hudson.test.JenkinsRule.WebClient;
import org.jvnet.hudson.test.recipes.LocalData;

import com.gargoylesoftware.htmlunit.html.HtmlPage;

import hudson.model.FreeStyleBuild;
import hudson.model.FreeStyleProject;
import hudson.model.Result;

/**
 * Tests for {@link FailureCauseColumn}.
 *
 * @author Vincent Latombe &lt;vincent@latombe.net&gt;
 */
public class FailureCauseColumnTest {

  /**
   * The Jenkins Rule.
   */
  @Rule
  //CS IGNORE VisibilityModifier FOR NEXT 1 LINES. REASON: Jenkins Rule
  public JenkinsRule j = new JenkinsRule();

  /**
   * Happy test case with a view containing a {@link FailureCauseColumn}, text option being disabled.
   *
   * @throws Exception
   *           if so
   */
  @LocalData
  @Test
  public void givenAViewWithTheFailureCauseColumnDisplayTheFirstFailureCauseAsTitle() throws Exception {
    FreeStyleProject fs = j.createFreeStyleProject("total_failure");
    fs.getBuildersList().add(new FailureBuilder());
    fs.save();

    FreeStyleBuild r = fs.scheduleBuild2(0).get();
    j.assertBuildStatus(Result.FAILURE, r);

    WebClient webClient = j.createWebClient();
    HtmlPage page = webClient.goTo("view/columnwithouttext");
    assertNotNull("Couldn't find the failure cause image in columnwithouttext view",
    		getFirstByXPath(page,"//img[@Title='Failure Builder']"));
    assertNull(getFirstByXPath(page,"//*[.='Failure Builder']"));
  }

  @SuppressWarnings("unchecked")
  public <X> X getFirstByXPath(HtmlPage page, String xpathExpr) {
      List< ? > results = page.getByXPath(xpathExpr);
      if (results.isEmpty()) {
          return null;
      }
      return (X) results.get(0);
  }

  /**
   * Happy test case with a view containing a {@link FailureCauseColumn}, text option being enabled.
   *
   * @throws Exception
   *           if so
   */
  @LocalData
  @Test
  public void givenAViewWithTheFailureCauseColumnWithTextDisplayTheFirstFailureCauseAsTitleAndText() throws Exception {
    FreeStyleProject fs = j.createFreeStyleProject("total_failure");
    fs.getBuildersList().add(new FailureBuilder());
    fs.save();

    FreeStyleBuild r = fs.scheduleBuild2(0).get();
    j.assertBuildStatus(Result.FAILURE, r);

    WebClient webClient = j.createWebClient();
    HtmlPage page = webClient.goTo("view/columnwithtext");
    assertNotNull("Couldn't find the failure cause image in columnwithtext view",
    		getFirstByXPath(page,"//img[@Title='Failure Builder']"));
    assertNotNull(getFirstByXPath(page,"//*[.='Failure Builder']"));
  }

}
