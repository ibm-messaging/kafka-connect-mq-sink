""" Custom gitlint rules

    These are Python implementations of some of the guidelines described in
    https://pages.github.ibm.com/the-playbook/developer-guide/delivering-code-command-line/#commit-message-format
"""

from gitlint.rules import CommitRule, RuleViolation
from gitlint.options import IntOption
import re



class SignedOffBy(CommitRule):
    """ This rule will enforce that each commit contains a "Signed-off-by" line with a name and email address.
        https://pages.github.ibm.com/the-playbook/developer-guide/delivering-code-command-line/#developer-certificate-of-origin
    """

    id = "QP1"
    name = "body-requires-signed-off-by"

    signoffregex = re.compile('^(DCO 1.1 )?Signed-off-by: .* <[^@ ]+@[^@ ]+\.[^@ ]+>')

    def validate(self, commit):
        for line in commit.message.body:
            if self.signoffregex.match(line):
                return

        return [RuleViolation(self.id, "Body does not contain a valid 'Signed-off-by' line. See https://pages.github.ibm.com/the-playbook/developer-guide/delivering-code-command-line/#developer-certificate-of-origin", line_nr=1)]


class GithubIssue(CommitRule):
    """ This rule will enforce that each commit is associated with a Github issue that explains what the commit is for.
    """

    id = "QP2"
    name = "commit-require-github-issue"

    githubissueregex = re.compile('(Resolves|Closes|Contributes to|Reverts):? [a-z\-/]*#[0-9]+')

    def validate(self, commit):
        for line in commit.message.body:
            if self.githubissueregex.match(line):
                return

        return [RuleViolation(self.id, "Body does not contain a github issue reference", line_nr=1)]



